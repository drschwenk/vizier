import yaml
from decorator import decorator
from functools import reduce
import operator

configuration_yml_fp = None

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


def _load_settings_raw(config_fp):
    with open(config_fp, 'r') as stream:
        try:
            task_config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            import sys
            print(exc)
            print('failed to load configuration file')
            sys.exit()
    return task_config


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
def configure(action, *args, **kwargs):
    if not configuration_yml_fp:
        print('no configuration file set')
        import sys
        sys.exit()
    configs = _load_settings_raw(configuration_yml_fp)
    _set_defaults(configs)
    _convert_setting_durations(configs)
    override_configs = kwargs.get('override', None)
    if override_configs:
        for setting in override_configs:
            set_by_path(configs, setting['keys'], setting['value'])
    kwargs.update({'configuration': configs})
    return action(*args, **kwargs)


def get_by_path(configs, key_path):
    return reduce(operator.getitem, key_path, configs)


def set_by_path(configs, key_path, value):
    get_by_path(configs, key_path[:-1])[key_path[-1]] = value

