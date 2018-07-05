import boto3
import threading
import datetime
import abc
from botocore.exceptions import ClientError


class MturkClient:
    def __init__(self, **kwargs):
        self.in_production = kwargs['in_production']
        environments = {
            "production": {
                "endpoint": "https://mturk-requester.us-east-1.amazonaws.com",
                "preview": "https://www.mturk.com/mturk/preview",
                "manage": "https://requester.mturk.com/mturk/manageHITs",
                "reward": "0.00"
            },
            "sandbox": {
                "endpoint": "https://mturk-requester-sandbox.us-east-1.amazonaws.com",
                "preview": "https://workersandbox.mturk.com/mturk/preview",
                "manage": "https://requestersandbox.mturk.com/mturk/manageHITs",
                "reward": "0.01"
            },
        }
        self.mturk_environment = environments['production'] if kwargs['in_production'] else environments['sandbox']
        session = boto3.Session(profile_name=kwargs['profile_name'])
        self.client = session.client(
            service_name='mturk',
            endpoint_url=self.mturk_environment['endpoint']
        )

    @classmethod
    def perform(cls, action, **kwargs):
        try:
            response = action(**kwargs)
            return response
        except ClientError as e:
            raise


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
                     for point in self._batch]
        self._queue.put(responses)


class GetAssignments(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'list_assignments_for_hit')

    def run(self):
        assignments = []
        for hit in self._batch:
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
        for hit in self._batch:
            submitted_assignments = [
                a for a in hit['Assignments'] if a['AssignmentStatus'] == 'Submitted']
            for assignment in submitted_assignments:
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
            self.action, HITId=h['HITId'], Revert=self.revert) for h in self._batch]
        self._queue.put(responses)


class ExpireHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'update_expiration_for_hit')
        self.exp_date = datetime.datetime(2001, 1, 1)

    def run(self):
        responses = [self.amt.perform(
            self.action, HITId=h['HITId'], ExpireAt=self.exp_date) for h in self._batch]
        self._queue.put(responses)


class DeleteHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(batch, target_queue, **kwargs)
        self.action = getattr(self.amt.client, 'delete_hit')

    def run(self):
        disposed_hits = [h for h in self._batch if h['HITStatus'] != 'Disposed']
        responses = [self.amt.client.perform(
            self.action, HITId=h['HITId']) for h in disposed_hits]
        self._queue.put(responses)
