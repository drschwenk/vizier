import boto3
import threading
import datetime
from botocore.exceptions import ClientError


class MturkClient:
    def __init__(self, **kwargs):
        self.in_sandbox = kwargs['in_sandbox']
        environments = {
            "live": {
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

        self.mturk_environment = environments['live'] if not kwargs['in_sandbox'] else environments['sandbox']

        session = boto3.Session(profile_name=kwargs['profile_name'])
        self.client = session.client(
            service_name='mturk',
            region_name='us-east-1',
            endpoint_url=self.mturk_environment['endpoint'],
            aws_access_key_id=kwargs['aws_access_key_id'],
            aws_secret_access_key=kwargs['aws_secret_access_key']
        )

    def perform(self, action, **kwargs):
        """
        internal helper function for creating a HIT
        :param params the parameters (required and optional) common to all HITs
        :param **kwargs any other parameters needed for a specific HIT type
        :return the created HIT object
        """
        try:
            # print(self.client)
            response = action(**kwargs)
            return response
        except ClientError as e:
            print(e)
            return None


class BotoThreadedOperation(threading.Thread):

    def __init__(self, **kwargs):
        self.amt = MturkClient(**kwargs)
        super().__init__()


class CreateHits(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(**kwargs)
        self.batch = batch
        self._queue = target_queue
        self.action = getattr(self.amt.client, 'create_hit')

    def run(self):
        responses = [self.amt.perform(self.action, **point) for point in self.batch]
        # responses = [self.amt.create_hit(**point) for point in self.batch]
        self._queue.put(responses)


class GetAssignments(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(**kwargs)
        self.batch = batch
        self._queue = target_queue

    def run(self):
        assignments = []
        for hit in self.batch:
            assignments.append(self.amt.client.list_assignments_for_hit(
                HITId=hit['HITId'],
                AssignmentStatuses=['Submitted', 'Approved'],
                MaxResults=10)
            )
        self._queue.put(assignments)


class ApproveAssignments(BotoThreadedOperation):
    def __init__(self, batch, target_queue, **kwargs):
        super().__init__(**kwargs)
        self.batch = batch
        self._queue = target_queue

    def run(self):
        responses = []
        for hit in self.batch:
            for assignment in hit['Assignments']:
                if assignment['AssignmentStatus'] == 'Submitted':
                    assignment_id = assignment['AssignmentId']
                    try:
                        responses.append(self.amt.client.approve_assignment(
                            AssignmentId=assignment_id,
                            RequesterFeedback='good',
                            OverrideRejection=False,
                        ))
                    except ClientError:
                        print(f'approve failed: {assignment_id}')

        self._queue.put(responses)


class ExpireHits(BotoThreadedOperation):
    def __init__(self, hits, **kwargs):
        super().__init__(**kwargs)
        self.hits = hits
        self.exp_date = datetime.datetime(2001, 1, 1)

    def run(self):
        responses = [self.amt.client.update_expiration_for_hit(HITId=h['HITId'], ExpireAt=self.exp_date)
                     for h in self.hits]
        return responses


class DeleteHits(BotoThreadedOperation):
    def __init__(self, hits, **kwargs):
        super().__init__(**kwargs)
        self.hits = hits

    def run(self):
        responses = []
        for h in self.hits:
            if h['HITStatus'] != 'Disposed':
                try:
                    self.amt.client.delete_hit(HITId=h['HITId'])
                except ClientError as e:
                    print(e)
        return responses







