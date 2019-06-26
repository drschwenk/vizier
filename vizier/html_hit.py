import copy
import jinja2
import xmltodict
from .utils import recall_template_args
from .config import MTURK_DATA_SCHEMA
from .qualifications import build_qualifications


def _create_html_hit_params(**kwargs):
    """
    creates a HIT for a question with the specified HTML
    # :param params a dict of the HIT parameters, must contain an "html" parameter
    # :return the created HIT object
    """
    hit_params = copy.deepcopy(kwargs['configuration']['hit_params'])
    frame_height = hit_params.pop('frame_height')
    question_html = _render_hit_html(**kwargs)
    hit_params['Question'] = _create_question_xml(question_html, frame_height)
    hit_params['QualificationRequirements'] = build_qualifications(**kwargs['configuration'])
    return hit_params


def _render_hit_html(**kwargs):
    from .log import logger
    interface_params = kwargs['configuration']['interface_params']
    logger.debug('rendering %s template', interface_params['template_file'])
    missing_args = recall_template_args(**kwargs).difference(set(kwargs.keys()))
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
        <HTMLQuestion xmlns="{MTURK_DATA_SCHEMA[turk_schema]}">
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
        from .log import logger
        logger.error(err)
        raise
