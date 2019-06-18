import logging


def init_logging(log_format='default', log_level='debug'):
    log_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING
    }
    base_logging_level = log_levels.get(log_level, None)
    if not base_logging_level:
        raise TypeError(f'{log_level} is an incorrect logging type!')
    if not logger.handlers:
        if log_format == 'default':
            log_format = '%(asctime)s: %(levelname)s: %(message)s \t[%(filename)s: %(lineno)d]'
        date_format = '%m/%d %I:%M:%S'
        formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
        log_out_path = 'iconary_metrics_task_output/metric_val_420_2/logs/test.log'
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
