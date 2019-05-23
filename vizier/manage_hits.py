from collections import defaultdict
import json
import xmltodict
from .client_tasks import amt_multi_action
from .utils import surface_hit_data


@amt_multi_action
@surface_hit_data
def get_grouped_assignments(hits, task_configs):
    """
    Retrieved assignments associated with _batch
    :param hits: list of AMT _batch
    :return: dict of AMT assignments
    """
    return 'GetAssignments', hits


def get_assignments(hits, task_configs):
    grouped_assignments = get_grouped_assignments(hits)
    assignments = [asg for hit in grouped_assignments for asg in hit.get('Assignments', [])]
    return [asg for asg in assignments if asg]


def get_assignment_metadata(assignments):
    pass


def get_and_extract_results(hits, task_configs):
    """
    Retrieves AMT assignments and extracts turker responses
    :param hits: list of AMT _batch
    :return: dict of task results keyed on their globalID
    """
    assignments = get_assignments(hits, task_configs)
    answers = _get_answers(assignments)
    return _extract_responses(answers)


@amt_multi_action
def approve_assignments(assignments, task_configs):
    """
    Approves assignments
    :param assignments: list of assignments to improve
    :return: AMT client responses
    """
    return 'ApproveAssignments', assignments


def approve_hits(hits, task_configs):
    """
    Approves all assignments associated with _batch
    :param hits : list of _batch to improve
    :return: AMT client responses
    """
    assignments = get_assignments(hits, task_configs)
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
        answer_text = raw_answer['QuestionFormAnswers']['Answer']['FreeText']
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


def get_all_hits(self):
    """
    Retrieves all of the current users HITs.
    This can be slow if a user has accumulated many thousands of HITs
    :return: all user HITs
    TODO: parallelize this
    """
    paginator = self.amt.client.get_paginator('list_hits')
    response_iterator = paginator.paginate(
        PaginationConfig={
            'PageSize': 100,
        }
    )
    response = []
    for resp in response_iterator:
        response.extend(resp['HITs'])
    return response


@amt_multi_action
def expire_hits(hits):
    """
    Sets hit expiration to a date in the past
    :param hits: hit batch to expire
    :return: AMT client responses
    """
    return 'ExpireHIts', hits


@amt_multi_action
@surface_hit_data
def delete_hits(hits):
    """
    Deletes (permanently removes) _batch
    :param hits: _batch to delete
    :return: AMT client responses
    """
    return 'DeleteHits', hits


def force_delete_hits(hits, task_configs, force=False):
    """
    Deletes (permanently removes) hit batch by first expiring them
    :param hits: batch batch to delete
    :param force: flag to overcome production warning
    :return: AMT client responses
    """
    in_production = task_configs['amt_client_params']['in_production']
    if not force and in_production:
        print('Careful with this in production. Override with force=True')
    response = expire_hits(hits)
    response += delete_hits(hits)
    return response


@amt_multi_action
@surface_hit_data
def set_hits_reviewing(hits):
    """
    Sets hit status to reviewing
    :param hits: _batch to set status of
    :return: AMT client responses
    """
    return 'UpdateHITsReviewStatus', hits#,  revert=False


@amt_multi_action
@surface_hit_data
def revert_hits_reviewable(hits):
    """
    Reverts hit reviewing status
    :param hits: _batch to revert
    :return: AMT client responses
    """
    return 'UpdateHITsReviewStatus', hits#,  revert=True)


@surface_hit_data
def get_assignable_hits(hits):
    hit_statuses = get_hit_statuses(hits)
    return [h for h in hit_statuses if h == 'Assignable']


def get_hit_statuses(hits, task_configs):
    import pandas as pd
    updated_hits = get_updated_hits(hits, task_configs)
    statuses = {h['HIT']['HITId']: h['HIT']['HITStatus'] for h in updated_hits}
    return pd.Series(statuses)


@amt_multi_action
@surface_hit_data
def get_updated_hits(hits, task_configs):
    return 'GetHITs', hits
