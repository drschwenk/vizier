# -*- coding: utf-8 -*-
""" AMT Client Interaction

"""
import queue
import threading
import abc
import boto3
from decorator import decorator
from botocore.exceptions import ClientError
from tqdm import tqdm
from .config import configure


@decorator
@configure
def amt_multi_action(amt_action, *args, **kwargs):
    client_config = kwargs['configuration']['amt_client_params']
    n_threads = client_config['n_threads']
    action_name, hit_batch = amt_action(*args, **kwargs)
    amt_action = globals().get(action_name)
    hit_batches = [hit_batch[i::n_threads] for i in range(n_threads)]
    threads = []
    res_queue = queue.Queue()
    for batch in hit_batches:
        thread = amt_action(batch, res_queue, **client_config, **kwargs)
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
    result_list = []
    while not res_queue.empty():
        result_list.append(res_queue.get())
    resp = [item for sub_l in result_list for item in sub_l]
    resp = list(filter(None, resp))
    print('\n' * (n_threads - 1))
    from .log import logger
    logger.info('performed %s/%s %s actions', len(resp), len(hit_batch), action_name)
    return resp


@decorator
@configure
def amt_serial_action(action, *args, **kwargs):
    client_config = kwargs['configuration']['amt_client_params']
    amt_client = MturkClient(**client_config).amt_client()
    action_name, request_batch = action(*args, **kwargs)
    client_action = getattr(amt_client, action_name)
    resp = [client_action(**req) for req in tqdm(request_batch)]
    resp = list(filter(None, resp))
    print('\n')
    from .log import logger
    logger.info('performed %s %s actions', len(resp), action_name)
    return resp


@decorator
@configure
def amt_single_action(action, *args, **kwargs):
    client_config = kwargs['configuration']['amt_client_params']
    amt_client = MturkClient(**client_config).amt_client()
    action_name, client_action_args = action(*args, **kwargs)
    client_action = getattr(amt_client, action_name)
    if not client_action_args:
        return client_action()
    resp = client_action(**client_action_args)
    from .log import logger
    logger.info('performed %s action', action_name)
    return resp


class MturkClient:
    def __init__(self, **kwargs):
        session = boto3.Session(profile_name=kwargs['profile_name'])
        in_production = kwargs.get('in_production', False)
        endpoints = {
            True: 'https://mturk-requester.us-east-1.amazonaws.com',
            False: 'https://mturk-requester-sandbox.us-east-1.amazonaws.com'
        }
        self.client = session.client(
            service_name='mturk',
            endpoint_url=endpoints[in_production],
        )

    def perform(self, action, **kwargs):
        allowed_exceptions = (
            ClientError,
            self.client.exceptions.RequestError,
        )
        try:
            response = action(**kwargs)
            return response
        except allowed_exceptions as err:
            from .log import logger
            logger.error('HITId: %s || %s', kwargs.get('HITId', ''), err)

    def amt_client(self):
        return self.client


class BotoThreadedOperation(threading.Thread):
    def __init__(self, batch, target_queue, **kwargs):
        self.amt = MturkClient(**kwargs)
        self._batch = batch
        self._queue = target_queue
        super().__init__()

    @abc.abstractmethod
    def run(self):
        responses = []
        self._queue.put(responses)


class CreateHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'create_hit')

    def run(self):
        responses = [self.amt.perform(self.action, **point)
                     for point in tqdm(self._batch)]
        self._queue.put(responses)


class GetHITs(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'get_hit')

    def run(self):
        hits = []
        for hit in tqdm(self._batch):
            action_args = {
                'HITId': hit['HITId'],
            }
            hits.append(self.amt.perform(self.action, **action_args))
        self._queue.put(hits)


class GetAssignments(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'list_assignments_for_hit')

    def run(self):
        assignments = []
        for hit in tqdm(self._batch):
            action_args = {
                'HITId': hit['HITId'],
                'AssignmentStatuses': ['Submitted', 'Approved'],
                'MaxResults': 50
            }
            assignments.append(self.amt.perform(self.action, **action_args))
        self._queue.put(assignments)


class ApproveAssignments(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'approve_assignment')

    def run(self):
        responses = []
        for assignment in tqdm(self._batch):
            action_args = {
                'AssignmentId': assignment['AssignmentId'],
                'RequesterFeedback': 'good',
                'OverrideRejection': False
            }
            responses.append(self.amt.perform(self.action, **action_args))
        self._queue.put(responses)


class UpdateHITsReviewStatus(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.revert = kwargs['revert']
        self.action = getattr(self.amt.client, 'update_hit_review_status')

    def run(self):
        responses = [self.amt.perform(
            self.action, HITId=h['HITId'], Revert=self.revert) for h in tqdm(self._batch)]
        self._queue.put(responses)


class ExpireHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        import datetime
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'update_expiration_for_hit')
        self.exp_date = datetime.datetime(2001, 1, 1)

    def run(self):
        responses = [self.amt.perform(
            self.action, HITId=h['HITId'], ExpireAt=self.exp_date) for h in tqdm(self._batch)]
        self._queue.put(responses)


class DeleteHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'delete_hit')

    def run(self):
        to_dispose_hits = [h for h in self._batch if h['HITStatus'] != 'Disposed']
        responses = [self.amt.perform(
            self.action, HITId=h['HITId']) for h in tqdm(to_dispose_hits)]
        self._queue.put(responses)
