# -*- coding: utf-8 -*-
"""HTML HIT Creation

Attributes:
     _MTURK_DATA_SCHEMA_BASE (str):
     _MTURK_DATA_SCHEMA (dict):
"""
import copy
import jinja2
import xmltodict
from .utils import recall_template_args
from .qualifications import build_qualifications


_MTURK_DATA_SCHEMA_BASE = 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/'
_MTURK_DATA_SCHEMA = {
    'html': ''.join([_MTURK_DATA_SCHEMA_BASE, '2011-11-11/HTMLQuestion.xsd']),
    'external': ''.join([_MTURK_DATA_SCHEMA_BASE, '2006-07-14/ExternalQuestion.xsd'])
}


def create_hit_params(**kwargs):
    hit_params = copy.deepcopy(kwargs['configuration']['hit_params'])
    frame_height = hit_params.pop('frame_height', '')
    hit_params['QualificationRequirements'] = build_qualifications(**kwargs['configuration'])
    return hit_params, frame_height


def create_html_hit_params(**kwargs):
    """
    :return
    """
    hit_params, frame_height = create_hit_params(**kwargs)
    question_html = render_hit_html(**kwargs)
    hit_params['Question'] = _create_question_xml(question_html, frame_height)
    return hit_params


def render_hit_html(**kwargs):
    """
    Creates Jinja environment and renders template using fields present in
    kwargs. Performs a check to assure all fields found in the template are provided.
    :return: HTML that will be used when creating and HTML Question HIT
    """
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


def _create_question_xml(question_html, frame_height, turk_schema='html'):
    """
    Embeds question HTML in AMT in HTMLQuestion XML schema
    :param question_html: HTML
    :param (int) frame_height: height of mturk Iframe
    :param (str) turk_schema: schema type- must be defined in _MTURK_DATA_SCHEMA
    :return: xml schema
    """
    hit_xml = f"""\
        <HTMLQuestion xmlns="{_MTURK_DATA_SCHEMA[turk_schema]}">
            <HTMLContent><![CDATA[
                <!DOCTYPE html>
                    {question_html}
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
