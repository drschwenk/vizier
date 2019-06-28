# -*- coding: utf-8 -*-
"""Logging

This module creates a logger object that is shared amongst other modules in the
package. The logging level is specified in the task_config.yml file, and
the log output path is generated dynamically based on the project name and batch id

Attributes:
     LOG_LEVELS (dict): map from debug names to level integers in the logging package
"""
import logging
# from .utils import prepare_output_path
import vizier.utils as utils
# from .config import configure
import vizier.config as config

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING
}


@config.configure
def init_logging(log_format='default', **kwargs):
    """
    Initializes logging with level and output paths
    :param (str) log_format: formatting string to use, or specify the default
    : **kwargs: Arbitrary keyword arguments, must contain configuration
    :return: None
    """
    task_config = kwargs['configuration']
    log_level = task_config['experiment_params']['debug_level']
    base_logging_level = LOG_LEVELS.get(log_level, None)
    if not base_logging_level:
        raise TypeError(f'{log_level} is an incorrect logging type!')
    if not logger.handlers:
        if log_format == 'default':
            log_format = '%(asctime)s: %(levelname)s: %(message)s \t[%(filename)s: %(lineno)d]'
        date_format = '%m/%d %I:%M:%S'
        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
        log_out_path = utils.prepare_output_path('log.txt', task_config, include_timestamp=False)
        chan = logging.StreamHandler()
        file_handler = logging.FileHandler(log_out_path)
        logger.setLevel(base_logging_level)
        file_handler.setLevel(base_logging_level)
        chan.setLevel(base_logging_level)
        chan.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        logger.addHandler(chan)
        logger.addHandler(file_handler)


logger = logging.getLogger(__name__)
init_logging()
