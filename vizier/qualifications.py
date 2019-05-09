master_qual_ids = {'production': '2F1QJWKUDD8XADTFD2Q0G6UTO95ALH',
                   'sandbox': '2ARFPLSP75KLA8M8DH1HTEQVJT3SY6'}

amt_environment = 'sandbox'


def build_qualifications(qualifications, **kwargs):
    """
    builds qualifications for task
    :param qualifications:
    :return: list of qualification dicts
    """
    global amt_environment
    amt_environment = 'production' if kwargs.get('in_production', False) else 'sandbox'
    quals = [_qual_builder(qual)(setting) for qual, setting in qualifications.items()]
    return list(filter(None, quals))


def _qual_builder(qualification):
    return globals().get(f'_{qualification}', _custom_qualification)


def _check_qual_inclusion(qual_builder):
    def inclusion_decorator(setting):
        if not setting or setting == 'false':
            return None
        if isinstance(setting, dict):
            from copy import deepcopy
            setting = deepcopy(setting)
            if setting.pop('active', '') != amt_environment:
                return None
        return qual_builder(setting)
    return inclusion_decorator


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
    master_qual_id = master_qual_ids.get(amt_environment, '')
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
