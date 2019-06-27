import os
import json
import gzip
import time
import pickle
from pprint import pprint
from decorator import decorator
from .amt_client import amt_single_action
from .config import configure


@decorator
def surface_hit_ids(action, *args, **kwargs):
    action_name, hits = action(*args, **kwargs)
    if hits and hits[0].get('HIT'):
        return action_name, [hit['HIT'] for hit in hits]
    return action_name, hits


@amt_single_action
def get_account_balance(**kwargs):
    return 'get_account_balance', None


def get_numerical_balance():
    balance_response = get_account_balance()
    return float(balance_response['AvailableBalance'])


def print_balance():
    balance = get_numerical_balance()
    print(f'Account balance is: ${balance:.{2}f}')


def make_standard_fig(fig_labels=None, save=False, outfile='fig.png', label_color='0.25'):
    import matplotlib.pylab as plt
    plt.rcParams['grid.linewidth'] = 0
    plt.rcParams['figure.figsize'] = (16.0, 10.0)
    if fig_labels:
        if 'fig_title' in fig_labels:
            plt.title(fig_labels['fig_title'], fontsize=15, verticalalignment='bottom', color=label_color)
        if 'y_label' in fig_labels:
            plt.ylabel(fig_labels['y_label'], fontsize=15, labelpad=10, color=label_color)
        if 'x_label' in fig_labels:
            plt.xlabel(fig_labels['x_label'], fontsize=12, labelpad=10, color=label_color)
    plt.xticks(rotation=45)
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.tick_params(axis='y', which='major', labelsize=10)
    plt.tight_layout()
    plt.show()
    if save:
        plt.savefig(outfile, bbox_inches='tight')


def recall_template_args(**kwargs):
    """
    Collects all of the arguments expected by the interface template
    at HIT creation
    : return (set): the expected arguments
    """
    import re
    template_dir = kwargs['configuration']['interface_params']['template_dir']
    template_fn = kwargs['configuration']['interface_params']['template_file']
    template_fp = os.path.join(template_dir, template_fn)
    with open(template_fp) as file:
        template_html = file.read()
    template_args = re.findall(r'\{\{(.*?)\}', template_html)
    return set(template_args)


@configure
def expected_cost(data, **kwargs):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :return: cost if sufficient funds, false if not
    """
    hit_params = kwargs['configuration']['hit_params']
    base_cost = float(hit_params['Reward'])
    n_assignments_per_hit = hit_params['MaxAssignments']
    fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
    min_fee_per_assignment = 0.01
    fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
    cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
    current_balance = get_numerical_balance()
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: batch will cost ${cost_plus_fee:.{2}f}',
            f'and only ${current_balance:.{2}f} available.',
            f'An additional ${cost_plus_fee - current_balance:.{2}f} is needed.'
        )
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')


@decorator
@configure
def serialize_action_result(action, *args, **kwargs):
    from .log import logger
    configs = kwargs['configuration']
    output_format = configs['serialization_params']['output_format']
    output_fp = _prepare_output_path(action, configs)
    compress = configs['serialization_params']['compress']
    res = action(*args, **kwargs)
    serialize_result(res, output_format, output_fp, compress=compress)
    logger.info('%s results written to %s', action.__name__, output_fp)
    return res


def serialize_result(result, output_format, output_fp, compress=False):
    available_serializers = {
        'json': _dump_json,
        'pickle': _dump_pickle
    }
    serializer = available_serializers.get(output_format, None)
    serializer(result, output_fp, compress)


@configure
def deserialize_result(input_fp, **kwargs):
    configs = kwargs['configuration']
    available_deserializers = {
        'json': _load_json,
        'pickle': _load_pickle
    }
    output_format = configs['serialization_params']['output_format']
    deserializer = available_deserializers.get(output_format, None)
    if deserializer:
        return deserializer(input_fp, compress=configs['serialization_params']['compress'])
    return None


def _prepare_output_path(action, configs, include_timestamp=True):
    output_dir_base = configs['serialization_params']['output_dir_base']
    experiment = configs['experiment_params']['batch_id']
    out_dir = os.path.join(output_dir_base, experiment)
    os.makedirs(out_dir, exist_ok=True)
    action_name = 'result--' + action.__name__ if callable(action) else action
    environment = None if configs['amt_client_params']['in_production'] else 'sbx'
    timestamp = _create_timestamp() if include_timestamp else None
    out_fn_components = [environment, action_name, timestamp]
    file_name = '--'.join(list(filter(None, out_fn_components)))
    output_fp = os.path.join(out_dir, file_name)
    return output_fp


def _create_timestamp():
    _, month, day, clock, year, = time.asctime().lower().split()
    hour, minute, _ = clock.split(':')
    timestamp = '_'.join([year, month, day, hour, minute])
    return timestamp


def _read(file_name, mode='rb'):
    with open(file_name, mode) as file:
        return file.read()


def _write(file_name, data, mode='wb'):
    with open(file_name, mode) as file:
        file.write(data)


def _write_compressed(file_name, data, compress_level):
    _write(f'{file_name}.gz', gzip.compress(data, compresslevel=compress_level))


def _append_file_ext(file_name, file_ext):
    if not file_name.endswith(file_ext):
        file_name = '.'.join([file_name, file_ext])
    return file_name


def _load_json(file_name, compress):
    file_name = _append_file_ext(file_name, 'json')
    if compress:
        return json.loads(gzip.decompress(_read(file_name)).decode('utf8'))
    return json.loads(_read(file_name, 'r'))


def _dump_json(dump_object, file_name, compress, indent=4, compress_level=9):
    file_name = _append_file_ext(file_name, 'json')
    if compress:
        data = json.dumps(dump_object, sort_keys=True, default=str)
        _write_compressed(file_name, data.encode('utf8'), compress_level)
    else:
        data = json.dumps(dump_object, sort_keys=True, indent=indent, default=str)
        _write(file_name, data, 'w')
    return dump_object


def _load_pickle(file_name, compress):
    file_name = _append_file_ext(file_name, 'pkl')
    data = _read(file_name)
    if compress:
        load_object = pickle.loads(gzip.decompress(data))
    else:
        load_object = pickle.loads(data)
    return load_object


def _dump_pickle(dump_object, file_name, compress, compress_level=9):
    file_name = _append_file_ext(file_name, 'pkl')
    data = pickle.dumps(dump_object)
    if compress:
        _write_compressed(file_name, data, compress_level)
    else:
        _write(file_name, data)
    return dump_object


def load_input_data(data_fp, compress=False):
    available_deserializers = {
        'json': _load_json,
        'pkl': _load_pickle
    }
    file_ext = os.path.splitext(data_fp)[-1].replace('.', '')
    data_loader = available_deserializers.get(file_ext, None)
    if not data_loader:
        raise NotImplementedError
    return data_loader(data_fp, compress)


def record_input_data(data, **kwargs):
    configs = kwargs['configuration']
    output_format = configs['serialization_params']['output_format']
    output_fp = _prepare_output_path('record--input_data', configs)
    compress = configs['serialization_params']['compress']
    serialize_result(data, output_format, output_fp, compress=compress)
    from .log import logger
    logger.info('recording HIT creation input data at %s', output_fp)
    return data


def record_template(**kwargs):
    import shutil
    interface_params = kwargs['configuration']['interface_params']
    template_fn = interface_params['template_file']
    template_dir = interface_params['template_dir']
    template_fp = os.path.join(template_dir, template_fn)
    template_fn = 'record--' + template_fn
    template_fn, f_ext = os.path.splitext(template_fn)
    output_fp = _prepare_output_path('record--template_generator', kwargs['configuration'])
    output_fp += f_ext
    shutil.copy(template_fp, output_fp)
    from .log import logger
    logger.info('recording template generator at %s', output_fp)


def record_template_generator(template_gen_fp, **kwargs):
    import shutil
    template_gen_fn = 'record--' + os.path.split(template_gen_fp)[-1]
    template_gen_fn, f_ext = os.path.splitext(template_gen_fn)
    output_fp = _prepare_output_path('record--jinja_template', kwargs['configuration'])
    output_fp += f_ext
    shutil.copy(template_gen_fp, output_fp)
    from .log import logger
    logger.info('recording HIT template at %s', output_fp)


def record_configuration(action, configs):
    import yaml
    out_fn = '--'.join(['record', 'config', action.__name__])
    output_fp = ''.join([_prepare_output_path(out_fn, configs), '.yml'])
    with open(output_fp, 'w') as stream:
        yaml.safe_dump(configs, stream)
    from .log import logger
    logger.info('recording HIT configuration at %s', output_fp)


@configure
def summarize_proposed_task(data, **kwargs):

    def add_space():
        for i in range(2):
            print('.')

    def add_section_break():
        print('-'.join([''] * 100))

    params_to_display = [
        'experiment_params',
        'hit_params',
        'amt_client_params'
    ]
    add_section_break()
    for param_type in params_to_display:
        print(param_type, ':')
        pprint(kwargs['configuration'][param_type])
        add_section_break()
    add_space()
    expected_cost(data)
    add_space()
    print_balance()
    add_space()
    in_prod = kwargs['configuration']['amt_client_params']['in_production']
    amt_environment = 'production' if in_prod else 'sandbox'
    print(f'task will be launched in {amt_environment.upper()}')
    add_space()


def confirm_action(prompt):
    while True:
        confirm = input(prompt)
        if confirm.lower() in ('y', 'n', 'yes', 'no'):
            if confirm[0] != 'y':
                import sys
                print('aborting...')
                sys.exit()
            break
        else:
            print("\n Invalid--Please enter y or n.")