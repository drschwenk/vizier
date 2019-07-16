# -*- coding: utf-8 -*-
"""Creation of HITs
"""
import os
from vizier.config import (
    configure,
)
from vizier.utils import (
    confirm_action,
)
from vizier.cost import summarize_proposed_task
from vizier.serialize import (
    load_interface_arg_generator,
    record_input_data,
    record_template,
    serialize_action_result
)
from vizier.amt_client import (
    amt_multi_action,
    amt_single_action
)
from vizier.html_hit import (
    create_html_hit_params,
    render_hit_html
)


@configure(record_config=True)
@serialize_action_result
@amt_multi_action
def create_hits(data, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data: task data
    :return: hit objects created
    """
    summarize_proposed_task(data, **kwargs)
    confirm_action(f'create {len(data)} hits with these settings? y/n\n')
    record_input_data(data, **kwargs)
    record_template(**kwargs)
    # from .log import logger
    # logger.info('recording HIT configuration at %s', output_fp)
    interface_arg_generator = load_interface_arg_generator(record=True, **kwargs)
    hit_batch = [create_html_hit_params(**interface_arg_generator(datum, **kwargs), **kwargs)
                 for datum in data]
    return 'CreateHits', hit_batch


@configure(record_config=True)
@amt_single_action
def create_single_hit(datum, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param datum: task data
    :return: hit objects created
    """
    confirm_action('launch single hit? y/n\n')
    interface_arg_generator = load_interface_arg_generator(**kwargs)
    single_hit = create_html_hit_params(**interface_arg_generator(datum, **kwargs), **kwargs, )
    return 'create_hit', single_hit


@configure
def preview_interface(datum, **kwargs):
    """
    :param datum:
    :return:
    """
    from .log import logger
    logger.info('preview')
    task_configs = kwargs['configuration']
    interface_arg_generator = load_interface_arg_generator(**kwargs)
    preview_dir = task_configs['interface_params']['preview_dir']
    preview_filename = ''.join([task_configs['experiment_params']['batch_id'], '.html'])
    preview_out_file = os.path.join(preview_dir, preview_filename)
    hit_html = render_hit_html(**interface_arg_generator(datum, **kwargs), **kwargs)
    if not os.path.exists(preview_dir):
        os.makedirs(preview_dir)
    with open(preview_out_file, 'w') as file:
        file.write(hit_html)
    return preview_out_file
