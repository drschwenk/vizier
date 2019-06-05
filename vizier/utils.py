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
def get_account_balance(task_configs):
    return 'get_account_balance', None


def get_numerical_balance(task_configs):
    balance_response = get_account_balance(task_configs)
    return float(balance_response['AvailableBalance'])


def print_balance(task_configs):
    balance = get_numerical_balance(task_configs)
    print(f'Account balance is: ${balance:.{2}f}')


def expected_cost(data, task_configs):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :param task_configs:
    :return: cost if sufficient funds, false if not
    """
    hit_params = task_configs['hit_params']
    base_cost = float(hit_params['Reward'])
    n_assignments_per_hit = hit_params['MaxAssignments']
    min_fee_per_assignment = 0.01
    fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
    fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
    cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
    current_balance = get_numerical_balance(task_configs)
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: batch will cost ${cost_plus_fee:.{2}f}',
            f'and only ${current_balance:.{2}f} available.',
            f'An additional ${cost_plus_fee - current_balance:.{2}f} is needed.'
        )
        return False
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')
    return cost_plus_fee


def load_config(config_fp):
    import yaml
    with open(config_fp, 'r') as stream:
        try:
            task_config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            task_config = None
    return task_config


def create_directories(directories):
    for current_dir in directories:
        os.makedirs(current_dir, exist_ok=True)


@decorator
@configure
def serialize_action_result(action, *args, **kwargs):
    configs = kwargs['configuration']
    available_serializers = {
        'json': dump_json,
        'pickle': dump_pickle
    }
    output_format = configs['serialization_params']['output_format']
    serializer = available_serializers.get(output_format, None)
    res = action(*args, **kwargs)
    if serializer:
        output_fp = prepare_output_path(action, configs)
        serializer(res, output_fp, compress=configs['serialization_params']['compress'])


def prepare_output_path(action, configs):
    output_dir_base = configs['serialization_params']['output_dir_base']
    experiment = configs['experiment_params']['experiment_name']
    out_dir = os.path.join(output_dir_base, experiment)
    os.makedirs(out_dir, exist_ok=True)
    action_name = action.__name__
    environment = 'prod' if configs['amt_client_params'] else 'sbox'
    timestamp = create_timestamp()
    file_name = '--'.join([environment, action_name, timestamp])
    output_fp = os.path.join(out_dir, file_name)
    return output_fp


def create_timestamp():
    _, month, day, clock, year, = time.asctime().lower().split()
    hour, minute, sec = clock.split(':')
    timestamp = '_'.join([year, month, day, hour, minute])
    return timestamp


def read(file_name, mode='rb'):
    with open(file_name, mode) as f:
        return f.read()


def write(file_name, data, mode='wb'):
    with open(file_name, mode) as f:
        f.write(data)


def write_compressed(file_name, data, compress_level, mode='wb'):
    write(f'{file_name}.gz', gzip.compress(data, compresslevel=compress_level))


def append_file_ext(file_name, file_ext):
    if not file_name.endswith(file_ext):
        file_name = '.'.join([file_name, file_ext])
    return file_name


def load_json(file_name, compress):
    file_name = append_file_ext(file_name, 'json')
    if compress:
        return json.loads(gzip.decompress(read(file_name)).decode('utf8'))
    else:
        return json.loads(read(file_name, 'r'))


def dump_json(dump_object, file_name, compress, indent=4, compress_level=9) :
    file_name = append_file_ext(file_name, 'json')
    if compress:
        data = json.dumps(dump_object, sort_keys=True)
        write_compressed(file_name, data.encode('utf8'), compress_level)
    else:
        data = json.dumps(dump_object, sort_keys=True, indent=indent)
        write(file_name, data, 'w')


def load_pickle(file_name, compress):
    file_name = append_file_ext(file_name, 'pkl')
    data = read(file_name)
    if compress:
        load_object = pickle.loads(gzip.decompress(data))
    else:
        load_object = pickle.loads(data)
    return load_object


def dump_pickle(dump_object, file_name, compress, compress_level=9):
    file_name = append_file_ext(file_name, 'pkl')
    data = pickle.dumps(dump_object)
    if compress:
        write_compressed(file_name, data, compress_level)
    else:
        write(file_name, data)
