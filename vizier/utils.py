import os
import json
import gzip
import time
import pickle
from decorator import decorator
from .client_tasks import amt_single_action
from .config import configure


@decorator
def surface_hit_data(action, *args, **kwargs):
    action_name, hits = action(*args, **kwargs)
    if hits and hits[0].get('HIT'):
        return action_name, [hit['HIT'] for hit in hits]
    else:
        return action_name, hits


@amt_single_action
def get_account_balance():
    return 'get_account_balance', None


@configure
def get_numerical_balance():
    balance_response = get_account_balance()
    return float(balance_response['AvailableBalance'])


def print_balance():
    balance = get_numerical_balance()
    print(f'Account balance is: ${balance:.{2}f}')


@configure
def recall_template_args(**kwargs):
    import re
    template_dir = kwargs['configuration']['interface_params']['template_dir']
    template_fn = kwargs['configuration']['interface_params']['template_file']
    template_fp = os.path.join(template_dir, template_fn)
    with open(template_fp) as f:
        template_html = f.read()
    template_args = re.findall(r'\{\{(.*?)\}', template_html)
    return set(template_args)


@configure
def expected_cost(data, **kwargs):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :param task_configs:
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
        return False
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')
    return cost_plus_fee


@decorator
def serialize_action_result(action, *args, **kwargs):
    configs = kwargs['configuration']
    available_serializers = {
        'json': _dump_json,
        'pickle': _dump_pickle
    }
    output_format = configs['serialization_params']['output_format']
    serializer = available_serializers.get(output_format, None)
    res = action(*args, **kwargs)
    if serializer:
        output_fp = _prepare_output_path(action, configs)
        serializer(res, output_fp, compress=configs['serialization_params']['compress'])
    return res


def _prepare_output_path(action, configs):
    output_dir_base = configs['serialization_params']['output_dir_base']
    experiment = configs['experiment_params']['experiment_name']
    out_dir = os.path.join(output_dir_base, experiment)
    os.makedirs(out_dir, exist_ok=True)
    action_name = action.__name__
    environment = 'prod' if configs['amt_client_params']['in_production'] else 'sbox'
    timestamp = _create_timestamp()
    file_name = '--'.join([environment, action_name, timestamp])
    output_fp = os.path.join(out_dir, file_name)
    return output_fp


def _create_timestamp():
    _, month, day, clock, year, = time.asctime().lower().split()
    hour, minute, sec = clock.split(':')
    timestamp = '_'.join([year, month, day, hour, minute])
    return timestamp


def _read(file_name, mode='rb'):
    with open(file_name, mode) as f:
        return f.read()


def _write(file_name, data, mode='wb'):
    with open(file_name, mode) as f:
        f.write(data)


def _write_compressed(file_name, data, compress_level, mode='wb'):
    _write(f'{file_name}.gz', gzip.compress(data, compresslevel=compress_level))


def _append_file_ext(file_name, file_ext):
    if not file_name.endswith(file_ext):
        file_name = '.'.join([file_name, file_ext])
    return file_name


def _load_json(file_name, compress):
    file_name = _append_file_ext(file_name, 'json')
    if compress:
        return json.loads(gzip.decompress(_read(file_name)).decode('utf8'))
    else:
        return json.loads(_read(file_name, 'r'))


def _dump_json(dump_object, file_name, compress, indent=4, compress_level=9) :
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
