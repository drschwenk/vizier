from decorator import decorator
from .config import configure
from .config import MASTER_QUAL_IDS

amt_environment = 'sandbox'


@configure
def build_qualifications(**kwargs):
    """
    builds qualifications for task
    :return: list of qualification dicts
    """
    global amt_environment
    qualifications = kwargs['qualifications']
    in_production = kwargs['amt_client_params']['in_production']
    amt_environment = 'production' if in_production else 'sandbox'
    quals = [_qual_builder(qual)(setting) for qual, setting in qualifications.items()]
    return list(filter(None, quals))


def _qual_builder(qualification):
    return globals().get(f'_{qualification}', _custom_qualification)


@decorator
def _check_qual_inclusion(qual_builder, *args, **kwargs):
    setting = args[0]
    if not setting or setting == 'false':
        return None
    if isinstance(setting, dict):
        from copy import deepcopy
        setting = deepcopy(setting)
        is_active = setting.pop('active', '')
        if is_active != amt_environment:
            return None
    return qual_builder(setting)


@_check_qual_inclusion
def _min_accept_rate(setting):
    return {
        'QualificationTypeId': '000000000000000000L0',
        'Comparator': 'GreaterThanOrEqualTo',
        'IntegerValues': [setting],
        'RequiredToPreview': True,
    }


@_check_qual_inclusion
def _min_total_hits_approved(setting):
    return {
        'QualificationTypeId': '00000000000000000040',
        'Comparator': 'GreaterThanOrEqualTo',
        'IntegerValues': [setting],
        'RequiredToPreview': True,
    }


@_check_qual_inclusion
def _master(setting):
    master_qual_id = MASTER_QUAL_IDS.get(amt_environment, '')
    if not master_qual_id:
        return None
    return {
        'QualificationTypeId': master_qual_id,
        'Comparator': 'EqualTo',
        'RequiredToPreview': True,
    }


@_check_qual_inclusion
def _locales(setting):
    import iso3166
    allowed_locations = iso3166.countries_by_alpha2
    return {
        'QualificationTypeId': '00000000000000000071',
        'Comparator': 'In',
        'LocaleValues': [{'Country': loc} for loc in setting if loc in allowed_locations],
        'RequiredToPreview': True,
    }


@_check_qual_inclusion
def _custom_qualification(settings):
    return settings
