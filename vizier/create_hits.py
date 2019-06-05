import os
import copy
import jinja2
import xmltodict

from .config import configure
from .utils import serialize_action_result
from .qualifications import build_qualifications
from .client_tasks import amt_multi_action
from .client_tasks import amt_single_action


turk_data_scheme_base = 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/'
turk_data_schemas = {
    'html': ''.join([turk_data_scheme_base, '2011-11-11/HTMLQuestion.xsd'])
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
        task_configs, **task_param_generator(point, task_configs)) for point in data]
    return 'CreateHits', hit_batch


@amt_single_action
def create_single_hit(data_point, task_param_generator, task_configs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data_point: task data
    :param task_param_generator: user-defined function to generate task parameters
    :param task_configs: task configuration
    :return: hit objects created
    """
    single_hit = _create_html_hit_params(task_configs, **task_param_generator(data_point, task_configs))
    return 'create_hit', single_hit


def preview_hit_interface(data_point, task_param_generator, task_configs):
    interface_params = task_configs['interface_params']
    preview_dir = task_configs['interface_params']['preview_dir']
    preview_filename = ''.join([task_configs['experiment_params']['experiment_name'], '.html'])
    preview_out_file = os.path.join(preview_dir, preview_filename)
    hit_html = _render_hit_html(interface_params, **task_param_generator(data_point, task_configs))
    if not os.path.exists(preview_dir):
        os.makedirs(preview_dir)
    with open(preview_out_file, 'w') as file:
        file.write(hit_html)


def expected_cost(data, task_config):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :param task_config: task configuration
    :return: cost if sufficient funds, false if not
    """
    hit_params = task_config['basic_hit_params']
    base_cost = float(hit_params['Reward'])
    n_assignments_per_hit = hit_params['MaxAssignments']
    min_fee_per_assignment = 0.01
    fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
    fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
    cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
    # current_balance = self.get_numerical_balance()
    current_balance = 1000
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: will cost ${cost_plus_fee:.{2}f} but only ${current_balance:.{2}f} available.')
        return False
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')
    return cost_plus_fee


@serialize_action_result
@configure
def _create_html_hit_params(**kwargs):
    """
    creates a HIT for a question with the specified HTML
    # :param params a dict of the HIT parameters, must contain an "html" parameter
    # :return the created HIT object
    """
    task_config = kwargs['configuration']
    basic_hit_params = task_config.get('hit_params', None)
    client_params = task_config.get('amt_client_params', None)
    in_production = client_params['in_production']
    interface_params = task_config.get('interface_params', None)
    qualification_params = task_config.get('qualifications')
    hit_params = copy.deepcopy(basic_hit_params)
    frame_height = hit_params.pop('frame_height')
    question_html = _render_hit_html(interface_params, **kwargs)
    hit_params['Question'] = _create_question_xml(
        question_html, frame_height)
    hit_params['QualificationRequirements'] = build_qualifications(qualification_params, in_production)
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



