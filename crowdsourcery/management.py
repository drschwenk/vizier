# -*- coding: utf-8 -*-
"""HIT Management

"""
from collections import defaultdict
import json
import xmltodict
from .amt_client import amt_multi_action
from .config import configure
from .utils import (
    confirm_action,
    surface_hit_ids
)
from .serialize import serialize_action_result


@amt_multi_action
@surface_hit_ids
def get_grouped_assignments(hits, **kwargs):
    """
    Retrieved assignments associated with _batch
    :param hits: list of AMT _batch
    :return: dict of AMT assignments
    """
    return 'GetAssignments', hits


@serialize_action_result
def get_assignments(hits, **kwargs):
    grouped_assignments = get_grouped_assignments(hits)
    assignments = [asg for hit in grouped_assignments for asg in hit.get('Assignments', [])]
    return [asg for asg in assignments if asg]


@serialize_action_result
def get_and_extract_results(hits, **kwargs):
    """
    Retrieves AMT assignments and extracts turker responses
    :param hits: list of AMT hits
    :return: dict of task results keyed on their globalID
    """
    assignments = get_assignments(hits)
    answers = _get_answers(assignments)
    return _extract_responses(answers)


@amt_multi_action
def approve_assignments(assignments, **kwargs):
    """
    Approves assignments
    :param assignments: list of assignments to improve
    :return: AMT client responses
    """
    return 'ApproveAssignments', assignments


def approve_hits(hits):
    """
    Approves all assignments associated with _batch
    :param hits : list of _batch to improve
    :return: AMT client responses
    """
    assignments = get_assignments(hits)
    return approve_assignments(assignments)


def _get_answers(assignments):
    """
    Extracts turker answers from assignments
    :param assignments: list of amt assignment objects
    :return: turker responses
    """
    answers = []
    for asg in assignments:
        raw_answer = xmltodict.parse(asg['Answer'])
        answer_text = raw_answer['QuestionFormAnswers']['Answer'].get('FreeText', None)
        if answer_text:
            answers.append(json.loads(answer_text))
        else:
            answers.append([])
    return answers


def _extract_responses(answers):
    """
    Extracts responses from answers
    :param answers: answers extracted from AMT assignments
    :return: dict of task results keyed on their globalID
    """
    results = defaultdict(list)
    for ans in answers:
        results[ans['globalID']].append(ans['results'])
    return dict(results)


@configure
def get_all_hits(**kwargs):
    """
    Retrieves all of the current users HITs.
    This can be slow if a user has accumulated many thousands of HITs
    :return: all user HITs
    """
    from .amt_client import MturkClient
    client_config = kwargs['configuration']['amt_client_params']
    amt_client = MturkClient(**client_config).amt_client()
    paginator = amt_client.get_paginator('list_hits')
    response_iterator = paginator.paginate(
        PaginationConfig={
            'PageSize': 100,
        }
    )
    response = []
    for resp in response_iterator:
        print('Getting next 100 hits')
        response.extend(resp['HITs'])
    return response


@amt_multi_action
@surface_hit_ids
def expire_hits(hits, **kwargs):
    """
    Sets hit expiration to a date in the past
    :param hits: hit batch to expire
    :return: AMT client responses
    """
    return 'ExpireHits', hits


@amt_multi_action
@surface_hit_ids
def delete_hits(hits, bypass=False, **kwargs):
    """
    Deletes (permanently removes) _batch
    :param bypass: bypass confirmation (used when calling from force_delete_hits
    :param hits: _batch to delete
    :return: AMT client responses
    """
    if not bypass:
        confirm_action(f'permanently delete {len(hits)} hits? y/n\n')
    return 'DeleteHits', hits


def force_delete_hits(hits, **kwargs):
    """
    Deletes (permanently removes) hit batch by first expiring them
    :param hits: batch batch to delete
    :return: AMT client responses
    """
    confirm_action(f'expire and permanently delete {len(hits)} hits? y/n\n')
    response = expire_hits(hits)
    response += delete_hits(hits, bypass=True)
    return response


@amt_multi_action
@surface_hit_ids
def change_hit_review_status(hits, **kwargs):
    """
    Sets hit status to reviewing
    :param hits: hits to set status of
    :param revert:
    :return: AMT client responses
    """
    return 'UpdateHITsReviewStatus', hits


@surface_hit_ids
def get_assignable_hits(hits):
    hit_statuses = get_hit_statuses(hits)
    return [h for h in hit_statuses if h == 'Assignable']


def get_hit_statuses(hits):
    import pandas as pd
    updated_hits = get_updated_hits(hits)
    statuses = {h['HIT']['HITId']: h['HIT']['HITStatus'] for h in updated_hits}
    return pd.Series(statuses)


@amt_multi_action
@surface_hit_ids
def get_updated_hits(hits, **kwargs):
    return 'GetHITs', hits
