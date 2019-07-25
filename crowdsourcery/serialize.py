import json
import gzip
import pickle
import os
import importlib.util
from decorator import decorator
from .config import configure
from .utils import prepare_output_path


@decorator
@configure
def serialize_action_result(action, *args, **kwargs):
    from .log import logger
    configs = kwargs['configuration']
    output_format = configs['serialization_params']['output_format']
    output_fp = prepare_output_path(action, configs)
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
    output_fp = prepare_output_path('record--input_data', configs)
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
    output_fp = prepare_output_path('record--template_generator', kwargs['configuration'])
    output_fp += f_ext
    shutil.copy(template_fp, output_fp)
    from .log import logger
    logger.info('recording template generator at %s', output_fp)


def record_template_generator(template_gen_fp, **kwargs):
    import shutil
    template_gen_fn = 'record--' + os.path.split(template_gen_fp)[-1]
    template_gen_fn, f_ext = os.path.splitext(template_gen_fn)
    output_fp = prepare_output_path('record--jinja_template', kwargs['configuration'])
    output_fp += f_ext
    shutil.copy(template_gen_fp, output_fp)
    from .log import logger
    logger.info('recording HIT template at %s', output_fp)


def load_interface_arg_generator(record=False, **kwargs):
    """

    :param record:
    :param kwargs:
    :return:
    """
    interface_params = kwargs['configuration']['interface_params']
    module_path = interface_params['template_arg_module']
    if record:
        record_template_generator(module_path, **kwargs)
    module_name = module_path.split('/')[-1].replace('.py', '')
    func_name = interface_params['template_arg_function']
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    task_spec_func = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(task_spec_func)
    return getattr(task_spec_func, func_name)




