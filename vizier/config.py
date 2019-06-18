import operator
from functools import reduce
import importlib.util
import yaml
from decorator import decorator

configuration_yml_fp = None

MASTER_QUAL_IDS = {
    'production': '2F1QJWKUDD8XADTFD2Q0G6UTO95ALH',
    'sandbox': '2ARFPLSP75KLA8M8DH1HTEQVJT3SY6'
}

MTURK_DATA_SCHEMA_BASE = 'http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/'
MTURK_DATA_SCHEMA = {
    'html': ''.join([MTURK_DATA_SCHEMA_BASE, '2011-11-11/HTMLQuestion.xsd']),
    'external': ''.join([MTURK_DATA_SCHEMA_BASE, '2006-07-14/ExternalQuestion.xsd'])
}

DEFAULT_SETTINGS = {
    'amt_client_params': {
        'in_production': False,
        'n_threads': 1,
        'profile_name': 'mturk_vision',
        's3_profile_name': 'default'
    },
    'hit_params': {
        'frame_height': 1170,
        'AssignmentDurationInHours': 1,
        'AutoApprovalDelayInHours': 48,
        'LifetimeInHours': 12
    },
    'interface_params': {
        'template_dir': 'hit_templates',
        'preview_dir': 'interface_preview'
    },
    'serialization_params': {
        'serialize': True,
        'output_dir_base': 'amt_output',
        'output_format': 'json',
        'compress': False
    },
    'qualifications': {
        'min_accept_rate': 95,
        'min_total_hits_approved': 1000,
        'master': False
    }
}


def _load_config_raw(config_fp):
    with open(config_fp, 'r') as stream:
        try:
            task_config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            import sys
            print(exc)
            print('failed to load configuration file')
            sys.exit()
    return task_config


def _dump_configs(action, configs):
    from .utils import _prepare_output_path
    out_fn = '--'.join([action.__name__, 'config.yml'])
    out_fp = _prepare_output_path(out_fn, configs)
    with open(out_fp, 'w') as stream:
        yaml.safe_dump(configs, stream)


def _set_defaults(raw_settings):
    for setting_cat, settings in raw_settings.items():
        for field, default in DEFAULT_SETTINGS.get(setting_cat, {}).items():
            if field not in settings:
                settings[field] = default


def _convert_setting_durations(raw_settings):
    seconds_per_hour = 3600
    to_convert = [
        'AssignmentDurationInHours',
        'AutoApprovalDelayInHours',
        'LifetimeInHours'
    ]
    raw_hit_params = raw_settings['hit_params']
    for field in to_convert:
        n_hours = raw_hit_params.pop(field)
        new_field = field.replace('Hours', 'Seconds')
        raw_hit_params[new_field] = n_hours * seconds_per_hour


def set_input_file_path(config_fp):
    global configuration_yml_fp
    configuration_yml_fp = config_fp
    print(f'using {configuration_yml_fp} for task configuration')


@decorator
def configure(action, record_config=False, *args, **kwargs):
    override_configs = kwargs.get('override', None)
    if 'configuration' in kwargs and not override_configs:
        configs = kwargs['configuration']
    else:
        if not configuration_yml_fp:
            print('no configuration file set')
            import sys
            sys.exit()
        configs = _load_config_raw(configuration_yml_fp)
        _set_defaults(configs)
        _convert_setting_durations(configs)
        if override_configs:
            for setting in override_configs:
                set_by_path(configs, setting['keys'], setting['value'])
        kwargs.update({'configuration': configs})
    if record_config:
        _dump_configs(action, configs)
    return action(*args, **kwargs)


def get_by_path(configs, key_path):
    reduce(operator.getitem, key_path, configs)


def set_by_path(configs, key_path, value):
    get_by_path(configs, key_path[:-1])[key_path[-1]] = value


def load_interface_arg_generator(**kwargs):
    interface_params = kwargs['configuration']['interface_params']
    module_path = interface_params['template_arg_module']
    module_name = module_path.split('/')[-1].replace('.py', '')
    func_name = interface_params['template_arg_function']
    mod_spec = importlib.util.spec_from_file_location(module_name, module_path)
    task_spec_func = importlib.util.module_from_spec(mod_spec)
    mod_spec.loader.exec_module(task_spec_func)
    return getattr(task_spec_func, func_name)
