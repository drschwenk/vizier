import os
import copy
import jinja2
import xmltodict

from .client_tasks import amt_multi_action
from .serialize import serialize
from .serialize import deserialize
from .qualifications import build_qualifications


turk_data_schemas = {
    'html': 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd'
    }


@amt_multi_action
def create_hit_group(data, task_param_generator, task_configs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data: task data
    :param task_param_generator: user-defined function to generate task parameters
    :param task_configs: task configuration
    :return: hit objects created
    """
    hit_batch = [_create_html_hit_params(
        task_configs, **task_param_generator(point)) for point in data]
    return 'CreateHits', hit_batch


def preview_hit_interface(template_params, html_dir='./interface_preview', page_name='task.html', **kwargs):
    hit_html = _render_hit_html(template_params, **kwargs)
    html_out_file = os.path.join(html_dir, page_name)
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    with open(html_out_file, 'w') as file:
        file.write(hit_html)


def expected_cost(data, **kwargs):
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
    # current_balance = self.get_num_balance()
    current_balance = 1000
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: will cost ${cost_plus_fee:.{2}f} but only ${current_balance:.{2}f} available.')
        return False
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')
    return cost_plus_fee


def _create_html_hit_params(task_config, **kwargs):
    """
    creates a HIT for a question with the specified HTML
    # :param params a dict of the HIT parameters, must contain an "html" parameter
    # :return the created HIT object
    """
    basic_hit_params = task_config.get('hit_params', None)
    interface_params = task_config.get('interface_params', None)
    qualification_params = task_config.get('qualifications')
    hit_params = copy.deepcopy(basic_hit_params)
    frame_height = hit_params.pop('frame_height')
    question_html = _render_hit_html(interface_params, **kwargs)
    hit_params['Question'] = _create_question_xml(
        question_html, frame_height)
    hit_params['QualificationRequirements'] = build_qualifications(qualification_params, **kwargs)
    return hit_params


def _render_hit_html(interface_params, **kwargs):
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(
        interface_params['template_dir']))
    template = env.get_template(interface_params['template_file'])
    return template.render(**kwargs)


def _create_question_xml(html_question, frame_height, turk_schema='html'):
    """
    Embeds question HTML in AMT HTMLQuestion XML
    :param html_question: task html
    :param frame_height: height of mturk iframe
    :param turk_schema: schema type
    :return:
    """
    hit_xml = f"""\
        <HTMLQuestion xmlns="{turk_data_schemas[turk_schema]}">
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
    except xmltodict.expat.ExpatError as err:
        print(err)
        raise



