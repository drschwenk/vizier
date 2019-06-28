# -*- coding: utf-8 -*-
"""Configuration

Attributes:
     configuration_yml_fp (str): path to task config yaml file.

     _DEFAULT_SETTINGS (dict):
"""
import operator
from functools import reduce

import yaml
from decorator import decorator

configuration_yml_fp = None




_DEFAULT_SETTINGS = {
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


def set_input_file_path(config_fp):
    """

    :param config_fp:
    :return:
    """
    global configuration_yml_fp
    configuration_yml_fp = config_fp
    print(f'using {configuration_yml_fp} for task configuration')
    return configuration_yml_fp


def _load_config_raw(config_fp):
    """

    :param config_fp:
    :return:
    """
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
    """

    :param raw_settings:
    :return:
    """
    for setting_cat, settings in raw_settings.items():
        for field, default in _DEFAULT_SETTINGS.get(setting_cat, {}).items():
            if field not in settings:
                settings[field] = default


def _convert_setting_durations(raw_settings):
    """

    :param raw_settings:
    :return:
    """
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


@decorator
def configure(action, record_config=False, *args, **kwargs):
    """

    :param action:
    :param record_config:
    :param args:
    :param kwargs:
    :return:
    """
    override_configs = kwargs.get('override', None)
    if 'configuration' in kwargs and not override_configs:
        configs = kwargs['configuration']
    else:
        if not configuration_yml_fp:
            print('no configuration file set\n')
            set_config_file()
        configs = _load_config_raw(configuration_yml_fp)
        _set_defaults(configs)
        _convert_setting_durations(configs)
        if override_configs:
            for setting in override_configs:
                set_by_path(configs, setting['keys'], setting['value'])
        kwargs.update({'configuration': configs})
    if record_config:
        record_configuration(action, configs)
    return action(*args, **kwargs)


def set_config_file():
    """

    :return:
    """
    import os
    prompt = 'please specify config file...\n'
    while True:
        config_fp = input(prompt)
        if os.path.exists(config_fp):
            set_input_file_path(config_fp)
            break
        else:
            print("\n Invalid path--Please enter again")


def record_configuration(action, configs):
    import yaml
    from vizier.utils import prepare_output_path
    out_fn = '--'.join(['record', 'config', action.__name__])
    output_fp = ''.join([prepare_output_path(out_fn, configs), '.yml'])
    with open(output_fp, 'w') as stream:
        yaml.safe_dump(configs, stream)


def get_by_path(configs, key_path):
    """

    :param configs:
    :param key_path:
    :return:
    """
    return reduce(operator.getitem, key_path, configs)


def set_by_path(configs, key_path, value):
    """

    :param configs:
    :param key_path:
    :param value:
    :return:
    """
    get_by_path(configs, key_path[:-1])[key_path[-1]] = value



