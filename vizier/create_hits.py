import os
import copy
import jinja2
import xmltodict

from .config import configure
from .utils import serialize_action_result
from .utils import recall_template_args
from .qualifications import build_qualifications
from .client_tasks import amt_multi_action
from .client_tasks import amt_single_action


TURK_DATA_SCHEMA_BASE = 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/'
TURK_DATA_SCHEMA = {
    'html': ''.join([TURK_DATA_SCHEMA_BASE, '2011-11-11/HTMLQuestion.xsd'])
    }


@configure
@serialize_action_result
@amt_multi_action
def create_hit_group(data, task_param_generator, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data: task data
    :param task_param_generator: user-defined function to generate task parameters
    :param task_configs: task configuration
    :return: hit objects created
    """
    hit_batch = [_create_html_hit_params(
        **kwargs, **task_param_generator(point, **kwargs)) for point in data]
    return 'CreateHits', hit_batch


@amt_single_action
def create_single_hit(data_point, task_param_generator, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data_point: task data
    :param task_param_generator: user-defined function to generate task parameters
    :param task_configs: task configuration
    :return: hit objects created
    """
    single_hit = _create_html_hit_params(**kwargs, **task_param_generator(data_point, **kwargs))
    return 'create_hit', single_hit


@configure
def preview_hit_interface(data_point, task_param_generator, **kwargs):
    task_configs = kwargs['configuration']
    interface_params = task_configs['interface_params']
    preview_dir = task_configs['interface_params']['preview_dir']
    preview_filename = ''.join([task_configs['experiment_params']['experiment_name'], '.html'])
    preview_out_file = os.path.join(preview_dir, preview_filename)
    hit_html = _render_hit_html(interface_params, **task_param_generator(data_point, **kwargs))
    if not os.path.exists(preview_dir):
        os.makedirs(preview_dir)
    with open(preview_out_file, 'w') as file:
        file.write(hit_html)


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
    missing_args = recall_template_args().difference(set(kwargs.keys()))
    if missing_args:
        print(f'{missing_args} are referenced in template but not supplied by template generator')
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
        <HTMLQuestion xmlns="{TURK_DATA_SCHEMA[turk_schema]}">
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



