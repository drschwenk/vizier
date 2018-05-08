import os
import copy
import queue
import xmltodict
import pickle
import json
import time
import jinja2
from collections import defaultdict
from botocore.exceptions import ClientError

from .client_tasks import MturkClient
from .client_tasks import CreateHits
from .client_tasks import GetAssignments
from .client_tasks import ExpireHits
from .client_tasks import ApproveAssignments


class Vizier:

    def __init__(self, **kwargs):
        """
        initializes a vizier instance with AWS credentials and a host
        :param aws_access_key_id the access key id.
        :param aws_secret_access_key the secret access key.
        :param host the mturk host to connect to
        """
        self.kwargs = kwargs
        self.amt = MturkClient(**self.kwargs)
        self.n_threads = kwargs['n_threads']
        self.in_sandbox = kwargs['in_sandbox']
        self.s3_base_path = kwargs['s3_base_path']
        self.turk_data_schemas = {
            'html': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd'
        }
        self.qualifications = {
            'high_accept_rate': 95,
            'english_speaking': ['US', 'CA', 'AU', 'NZ', 'GB'],
            'us_only': ['US'],
            'master': 'False'
        }
        self.print_balance()

    def get_num_balance(self):
        try:
            balance_response = self.amt.client.get_account_balance()
            return float(balance_response['AvailableBalance'])
        except ClientError as e:
            raise

    def print_balance(self):
        balance = self.get_num_balance()
        print(f'Account balance is: ${balance:.{2}f}')

    @classmethod
    def pickle_this(cls, this, filename='temp', protocol=pickle.HIGHEST_PROTOCOL, timestamp=''):
        if timestamp:
            timestamp = '_'.join(
                time.asctime().lower().replace(':', '_').split())
            filename = '_'.join([filename, timestamp])
        if not filename.endswith('.pkl'):
            filename += '.pkl'
        with open(filename, 'wb') as f:
            pickle.dump(this, f, protocol=protocol)

    @classmethod
    def unpickle_this(cls, filename):
        with open(filename, 'rb') as f:
            return pickle.load(f)

    @classmethod
    def _render_hit_html(cls, template_params, **kwargs):
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(
            template_params['template_dir']))
        template = env.get_template(template_params['template_file'])
        return template.render(**kwargs)

    def preview_hit_interface(self, template_params, html_dir='./interface_preview', page_name='task.html', **kwargs):
        hit_html = self._render_hit_html(template_params, **kwargs)
        html_out_file = os.path.join(html_dir, page_name)
        if not os.path.exists(html_dir):
            os.makedirs(html_dir)
        with open(html_out_file, 'w') as f:
            f.write(hit_html)

    def expected_cost(self, data, **kwargs):
        hit_params = kwargs['basic_hit_params']
        cost = len(data) * \
            float(hit_params['Reward']) * hit_params['MaxAssignments']
        cost_plus_fee = cost * 1.2  # account for 20% Amazon fee
        current_balance = self.get_num_balance()
        if cost_plus_fee > current_balance:
            print(
                f'Insufficient funds: will cost ${cost_plus_fee:.{2}f} but only ${current_balance:.{2}f} available.')
            return False
        else:
            print(f'Batch will cost ${cost_plus_fee:.{2}f}')
            return cost_plus_fee

    def _build_qualifications(self, locales=None):
        if locales:
            locales = [{'Country': loc} for loc in locales]
        masters_id = '2ARFPLSP75KLA8M8DH1HTEQVJT3SY6' if self.in_sandbox else '2F1QJWKUDD8XADTFD2Q0G6UTO95ALH'
        master = {
            'QualificationTypeId': masters_id,
            'Comparator': 'EqualTo',
            'RequiredToPreview': True,
        }
        high_accept_rate = {
            'QualificationTypeId': '000000000000000000L0',
            'Comparator': 'GreaterThanOrEqualTo',
            'IntegerValues': [self.qualifications['high_accept_rate']],
            'RequiredToPreview': True,
        }
        location_based = {
            'QualificationTypeId': '00000000000000000071',
            'Comparator': 'In',
            'LocaleValues': locales,
            'RequiredToPreview': True,
        }
        return [high_accept_rate, location_based]

    def _create_question_xml(self, html_question, frame_height, turk_schema='html'):
        hit_xml = f"""\
            <HTMLQuestion xmlns="{self.turk_data_schemas[turk_schema]}">
                <HTMLContent><![CDATA[
                    <!DOCTYPE html>
                        {html_question}
                    ]]>
                </HTMLContent>
                <FrameHeight>{frame_height}</FrameHeight>
            </HTMLQuestion>"""
        try:
            xmltodict.parse(hit_xml)
            return hit_xml
        except xmltodict.expat.ExpatError as e:
            print(e)
            raise

    def _create_html_hit_params(self, basic_hit_params, template_params, **kwargs):
        """
        creates a HIT for a question with the specified HTML
        # :param params a dict of the HIT parameters, must contain an "html" parameter
        # :return the created HIT object
        """
        hit_params = copy.deepcopy(basic_hit_params)
        frame_height = hit_params.pop('frame_height')
        question_html = self._render_hit_html(template_params, **kwargs)
        hit_params['Question'] = self._create_question_xml(
            question_html, frame_height)
        hit_params['QualificationRequirements'] = self._build_qualifications(
            self.qualifications['english_speaking'])
        return hit_params

    def _exec_task(self, hits, task):
        hit_batches = [hits[i::self.n_threads] for i in range(self.n_threads)]
        threads = []
        res_queue = queue.Queue()
        for batch in hit_batches:
            t = task(batch, res_queue, **self.kwargs)
            threads.append(t)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        result_list = []
        while not res_queue.empty():
            result_list.append(res_queue.get())
        return [item for sl in result_list for item in sl]

    def create_hit_group(self, data, task_param_generator, **kwargs):
        if not self.expected_cost(data, **kwargs):
            return None
        hit_params = [self._create_html_hit_params(
            **kwargs, **task_param_generator(point)) for point in data]
        hits_created = self._exec_task(hit_params, CreateHits)
        submission_type = 'sandbox_' if self.in_sandbox else 'production_'
        self.pickle_this(
            hits_created, f'submitted_batch_{submission_type + str(len(hits_created))}', timestamp='append')
        return hits_created

    @classmethod
    def _get_answers(cls, assignments):
        answers = []
        for hit in assignments:
            for asg in hit['Assignments']:
                answer_raw = xmltodict.parse(asg['Answer'])
                answers.append(json.loads(
                    answer_raw['QuestionFormAnswers']['Answer']['FreeText']))
        return answers

    @classmethod
    def _extract_responses(cls, answers):
        results = defaultdict(list)
        for ans in answers:
            results[ans['globalID']].append(ans['results'])
        return dict(results)

    def get_assignments(self, hits=()):
        return self._exec_task(hits, GetAssignments)

    def get_and_extract_results(self, hits=()):
        assignments = self.get_assignments(hits)
        answers = self._get_answers(assignments)
        return self._extract_responses(answers)

    def approve_assignments(self, assignments):
        return self._exec_task(assignments, ApproveAssignments)

    def get_all_hits(self):
        paginator = self.amt.client.get_paginator('list_hits')
        response_iterator = paginator.paginate(
            PaginationConfig={
                'PageSize': 100,
            }
        )
        response = []
        for r in response_iterator:
            response.extend(r['HITs'])
        return response

    def expire_hits(self, hits):
        return self._exec_task(hits, ExpireHits)

    def delete_hits(self, hits):
        responses = []
        for h in hits:
            if h['HITStatus'] != 'Disposed':
                try:
                    responses.append(self.amt.client.delete_hit(HITId=h['HITId']))
                except ClientError as e:
                    print(e)
        return responses

    def force_delete_hits(self, hits):
        self.expire_hits(hits)
        self.delete_hits(hits)

    # def set_hits_reviewing(self, hits):
    #     responses = [self.amt.client.update_hit_review_status(HITId=h['HITId'], Revert=False) for h in hits]

    # def revert_hits_reviewable(self, hits):
    #      responses = [self.client.update_hit_review_status(HITId=h['HITId'], Revert=True) for h in hits]
