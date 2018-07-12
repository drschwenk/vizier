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
from .client_tasks import ApproveAssignments
from .client_tasks import UpdateHITsReviewStatus
from .client_tasks import ExpireHits
from .client_tasks import DeleteHits


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
        self.in_production = kwargs['in_production']
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
    def pickle_this(cls, this, filename='temp', protocol=pickle.HIGHEST_PROTOCOL, timestamp=False):
        """
        Util function to pickle objects, with the option of appending a timestamp
        :param this: object to pickle
        :param filename: filename of pickle object
        :param protocol: pickle protocol to use
        :param timestamp: option to append timestamp to filename
        :return:
        """
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
        """
        Util function to unpickle objects
        :param filename: pickle file path
        :return: unpickled object
        """
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
        """
        Computes the expected cost of a hit batch
        To adjust for subtleties of the amt fees, see:
        www.mturk.com/pricing
        :param data: task data
        :param kwargs:
        :return: cost if sufficient funds, false if not
        """
        hit_params = kwargs['basic_hit_params']
        base_cost = float(hit_params['Reward'])
        n_assignments_per_hit = hit_params['MaxAssignments']
        min_fee_per_assignment = 0.01
        fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
        fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
        cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
        current_balance = self.get_num_balance()
        if cost_plus_fee > current_balance:
            print(
                f'Insufficient funds: will cost ${cost_plus_fee:.{2}f} but only ${current_balance:.{2}f} available.')
            return False
        else:
            print(f'Batch will cost ${cost_plus_fee:.{2}f}')
            return cost_plus_fee

    def _build_qualifications(self, locales=None):
        """
        builds qualifications for task
        :param locales: AMT country codes allowed to perform task
        :return: list of qualification dicts
        """
        if locales:
            locales = [{'Country': loc} for loc in locales]
        masters_id = '2F1QJWKUDD8XADTFD2Q0G6UTO95ALH' if self.in_production else '2ARFPLSP75KLA8M8DH1HTEQVJT3SY6'
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
        iconary = {
            'QualificationTypeId': '3Z1HL5WC7LSXUFW49BUXR7ZP1IDH6M',
            'Comparator': 'EqualTo',
            'ActionsGuarded': 'DiscoverPreviewAndAccept',
            'IntegerValues': [1]
        }
        return [high_accept_rate, location_based, iconary]

    def _create_question_xml(self, html_question, frame_height, turk_schema='html'):
        """
        Embeds question HTML in AMT HTMLQuestion XML
        :param html_question: task html
        :param frame_height: height of mturk iframe
        :param turk_schema: schema type
        :return:
        """
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

    def _exec_task(self, hits, task, **kwargs):
        """
        Executes task on _batch over multiple threads
        :param hits: _batch to perform task on
        :param task: vizier task function
        :param kwargs:
        :return: AMT client responses
        """
        hit_batches = [hits[i::self.n_threads] for i in range(self.n_threads)]
        threads = []
        res_queue = queue.Queue()
        combined_args = {**kwargs, **self.kwargs}
        for batch in hit_batches:
            t = task(batch, res_queue, **combined_args)
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
        """
        Creates a group of HITs from data and supplied generator and pickles resultant _batch
        :param data: task data
        :param task_param_generator: user-defined function to generate task parameters
        :param kwargs:
        :return: hit objects created
        """
        if not self.expected_cost(data, **kwargs):
            return None
        hit_params = [self._create_html_hit_params(
            **kwargs, **task_param_generator(point)) for point in data]
        hits_created = self._exec_task(hit_params, CreateHits)
        submission_type = 'production_' if self.in_production else 'sandbox_'
        self.pickle_this(
            hits_created, f'submitted_batch_{submission_type + str(len(hits_created))}', timestamp=True)
        return hits_created

    @classmethod
    def _get_answers(cls, assignments):
        """
        Extracts turker answers from assignments
        :param assignments: list of amt assignment objects
        :return: turker responses
        """
        answers = []
        for hit in assignments:
            for asg in hit['Assignments']:
                answer_raw = xmltodict.parse(asg['Answer'])
                answers.append(json.loads(
                    answer_raw['QuestionFormAnswers']['Answer']['FreeText']))
        return answers

    @classmethod
    def _extract_responses(cls, answers):
        """
        Extracts responses from answers
        :param answers: answers extracted from AMT assignments
        :return: dict of task results keyed on their globalID
        """
        results = defaultdict(list)
        for ans in answers:
            results[ans['globalID']].append(ans['results'])
        return dict(results)

    def get_assignments(self, hits=()):
        """
        Retrieved assignments associated with _batch
        :param hits: list of AMT _batch
        :return: dict of AMT assignments
        """
        return self._exec_task(hits, GetAssignments)

    def get_and_extract_results(self, hits=()):
        """
        Retrieves AMT assignments and extracts turker responses
        :param hits: list of AMT _batch
        :return: dict of task results keyed on their globalID
        """
        assignments = self.get_assignments(hits)
        answers = self._get_answers(assignments)
        return self._extract_responses(answers)

    def approve_assignments(self, assignments):
        """
        Approves assignments
        :param assignments: list of assignments to improve
        :return: AMT client responses
        """
        return self._exec_task(assignments, ApproveAssignments)

    def approve_hits(self, hits):
        """
        Approves all assignments associated with _batch
        :param hits : list of _batch to improve
        :return: AMT client responses
        """
        assignments = self.get_assignments(hits)
        return self._exec_task(assignments, ApproveAssignments)

    def get_all_hits(self):
        """
        Retrieves all of the current users HITs.
        This can be slow if a user has accumulated many thousands of HITs
        :return: all user HITs
        """
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
        """
        Sets hit expiration to a date in the past
        :param hits: _batch to expire
        :return: AMT client responses
        """
        return self._exec_task(hits, ExpireHits)

    def delete_hits(self, hits):
        """
        Deletes (permanently removes) _batch
        :param hits: _batch to delete
        :return: AMT client responses
        """
        return self._exec_task(hits, DeleteHits)

    def force_delete_hits(self, hits, force=False):
        """
        Deletes (permanently removes) _batch by first expiring them
        :param hits: _batch to delete
        :param force: flag to overcome production warning
        :return: AMT client responses
        """
        if not force and self.in_production:
            print('Careful with this in production. Override with force=True')
            return
        response = self.expire_hits(hits)
        response += self.delete_hits(hits)
        return response

    def set_hits_reviewing(self, hits):
        """
        Sets hit status to reviewing
        :param hits: _batch to set status of
        :return: AMT client responses
        """
        return self._exec_task(hits, UpdateHITsReviewStatus, revert=False)

    def revert_hits_reviewable(self, hits):
        """
        Reverts hit reviewing status
        :param hits: _batch to revert
        :return: AMT client responses
        """
        return self._exec_task(hits, UpdateHITsReviewStatus, revert=True)

    def create_qualification(self, **kwargs):
        """
        Creates a new task qualification ID
        :param kwargs: name, keywords, description, status, etc.
        :return: qualification ID string
        """
        return self.amt.client.create_qualification_type(**kwargs)

    def grant_qualification_to_workers(self, qualification_id, worker_ids, notify=True):
        """
        Grants qualification to workers
        :param qualification_id: qualification ID
        :param worker_ids: list of worker IDs
        :param notify: send notification email to workers
        :return:
        """
        responses = []
        for w_id in worker_ids:
            responses.append(self.amt.client.associate_qualification_with_worker(
                QualificationTypeId=qualification_id,
                WorkerId=w_id,
                IntegerValue=1,
                SendNotification=notify
            ))
        return responses

    def remove_qualification_from_workers(self, qualification_id, worker_ids, reason=''):
        """
        Revokes a worker's qualification
        :param qualification_id: qualification ID
        :param worker_ids: list of worker IDs
        :param reason: reason for disqualification to give workers
        :return:
        """
        responses = []
        for w_id in worker_ids:
            responses.append(self.amt.client.disassociate_qualification_with_worker(
                QualificationTypeId=qualification_id,
                WorkerId=w_id,
                Reason=reason
            ))
        return responses

    def message_workers(self, worker_ids, subject, message):
        """
        Messages a list of workers with a supplied message.
        :param worker_ids: list of worker IDs to message
        :param subject: subject to display in message
        :param message:
        :return: AMT client responses
        """
        batch_length = 100      # this is the maximum number of workers AMT allows in one notification
        n_batches = len(worker_ids) // batch_length + \
            bool(len(worker_ids) % batch_length)
        worker_batches = [worker_ids[i::n_batches] for i in range(n_batches)]
        response = []
        for workers in worker_batches:
            response.append(self.amt.client.notify_workers(
                Subject=subject,
                MessageText=message,
                WorkerIds=workers))
        return response

    def send_bonuses(self, worker_bonus_assignments, amount, reason):
        responses = []
        for worker_id, assignments in worker_bonus_assignments.items():
            for a_id in assignments:
                responses.append(self.amt.client.send_bonus(
                    WorkerId=worker_id,
                    BonusAmount=str(amount),
                    AssignmentId=a_id,
                    Reason=reason,
                    # UniqueRequestToken='string'
                ))
        return responses

