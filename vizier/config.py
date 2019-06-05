import yaml
from decorator import decorator

configuration_yml_fp = None

DEFAULT_SETTINGS = {
    'amt_client_params': {
        'in_production': False,
        'n_threads': 1,
        'profile_name': 'mturk_vision'
    },
    'interface_params': {
        'template_dir': 'hit_templates',
        'preview_dir': 'interface_preview'
    },
    'serialization_params': {
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
            task_config = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            task_config = None
    return task_config


def _set_defaults(raw_settings):

    for setting_cat, settings in raw_settings.items():
        for field, default in DEFAULT_SETTINGS.get(setting_cat, {}).items():
            if field not in settings:
                settings[field] = default


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
    # global experiment_params
    # experiment_params = settings.get('experiment_params')
    # global amt_client_params
    # amt_client_params = settings.get('amt_client_params')
    # global hit_params
    # hit_params = settings.get('hit_params')
    # global interface_params
    # interface_params = settings.get('interface_params')
    # global serialization_params
    # serialization_params = settings.get('serialization_params')
    # global qualifications
    # qualifications = settings.get('qualifications')
    kwargs.update({'configuration': configs})
    return action(*args, **kwargs)
