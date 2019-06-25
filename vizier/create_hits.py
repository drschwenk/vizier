import os
from .config import configure
from .config import load_interface_arg_generator
from .utils import serialize_action_result
from .utils import confirm_action
from .utils import summarize_proposed_task
from .client_tasks import amt_multi_action
from .client_tasks import amt_single_action
from .html_hit import _create_html_hit_params
from .html_hit import _render_hit_html


@configure(record_config=True)
@serialize_action_result
@amt_multi_action
def create_hit_group(data, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param data: task data
    :return: hit objects created
    """
    summarize_proposed_task(data)
    confirm_action('launch task with these settings? y/n\n')
    confirm_action(f'create {len(data)} hits? y/n\n')
    interface_arg_generator = load_interface_arg_generator(**kwargs)
    hit_batch = [_create_html_hit_params(**interface_arg_generator(datum, **kwargs), **kwargs)
                 for datum in data]
    return 'CreateHits', hit_batch


@amt_single_action
def create_single_hit(datum, **kwargs):
    """
    Creates a group of HITs from data and supplied generator and pickles resultant _batch
    :param datum: task data
    :return: hit objects created
    """
    confirm_action('launch single hit? y/n\n')
    interface_arg_generator = load_interface_arg_generator(**kwargs)
    single_hit = _create_html_hit_params(**interface_arg_generator(datum, **kwargs), **kwargs, )
    return 'create_hit', single_hit


@configure(record_config=True)
def preview_hit_interface(datum, **kwargs):
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
    hit_html = _render_hit_html(**interface_arg_generator(datum, **kwargs), **kwargs)
    if not os.path.exists(preview_dir):
        os.makedirs(preview_dir)
    with open(preview_out_file, 'w') as file:
        file.write(hit_html)
    return preview_out_file


def preserve_task_input():
    pass

