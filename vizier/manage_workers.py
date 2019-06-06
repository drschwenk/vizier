from .client_tasks import amt_serial_action
from .client_tasks import amt_single_action
from .config import configure
from .utils import serialize_action_result


@configure
@serialize_action_result
@amt_single_action
def create_qualification(qualification, **kwargs):
    """
    Creates a new task qualification ID. The format of the qualification should follow:
    response = client.create_qualification_type(
        Name='string',
        Keywords='string',
        Description='string',
        QualificationTypeStatus='Active'|'Inactive',
        RetryDelayInSeconds=123,
        Test='string',
        AnswerKey='string',
        TestDurationInSeconds=123,
        AutoGranted=True|False,
        AutoGrantedValue=123
    )
    :param qualification: name, keywords, description, status, etc.
    :return: qualification ID string
    """
    return 'create_qualification_type', qualification


@configure
@amt_serial_action
def grant_qualification_to_workers(qualification_id, worker_ids, notify=True):
    """
    Grants qualification to workers
    :param qualification_id: qualification ID
    :param worker_ids: list of worker IDs
    :param notify: send notification email to workers
    :return:
    """
    requests = []
    for w_id in worker_ids:
        requests.append({
            'QualificationTypeId': qualification_id,
            'WorkerId': w_id,
            'IntegerValue': 1,
            'SendNotification': notify
        })
    return 'associate_qualification_with_worker', requests


@configure
@amt_serial_action
def remove_qualification_from_workers(qualification_id, worker_ids, reason=''):
    """
    Revokes a worker's qualification
    :param qualification_id: qualification ID
    :param worker_ids: list of worker IDs
    :param reason: reason for disqualification to give workers
    :return:
    """
    requests = []
    for w_id in worker_ids:
        requests.append({
            'QualificationTypeId': qualification_id,
            'WorkerId': w_id,
            'Reason': reason
        })
    return 'disassociate_qualification_with_worker', requests


@configure
@amt_serial_action
def message_workers(worker_ids, subject, message):
    """
    Messages a list of workers with a supplied message.
    :param worker_ids: list of worker IDs to message
    :param subject: subject to display in message
    :param message:
    :return: AMT client responses
    """
    batch_length = 100      # this is the maximum number of workers AMT allows in one notification
    n_workers = len(worker_ids)
    n_batches, rem = divmod(n_workers, batch_length)
    n_batches += bool(rem)

    worker_batches = [worker_ids[i::n_batches] for i in range(n_batches)]
    requests = []
    for workers in worker_batches:
        requests.append({
            'Subject': subject,
            'MessageText': message,
            'WorkerIds': workers})
    return 'notify_workers', requests


@configure
@amt_serial_action
def send_bonuses(worker_bonus_assignments, amounts, reason):
    requests = []
    for worker_id, assignments in worker_bonus_assignments.items():
        amount = amounts[worker_id]
        for a_id in assignments:
            requests.append({
                'WorkerId': worker_id,
                'BonusAmount': str(amount),
                'AssignmentId': a_id,
                'Reason': reason,
            })
    return 'send_bonus', requests
