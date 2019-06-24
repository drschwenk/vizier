import logging
from .utils import _prepare_output_path
from .config import configure

LOG_LEVELS = {
    'debug': logging.DEBUG,
    'info': logging.INFO,
    'warning': logging.WARNING
}


@configure
def init_logging(log_format='default', **kwargs):
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
        log_out_path = _prepare_output_path('log.txt', task_config, include_timestamp=False)
        print(log_out_path)
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
