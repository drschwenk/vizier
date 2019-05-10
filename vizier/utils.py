from .client_tasks import amt_single_action


@amt_single_action
def get_account_balance(task_configs):
    return 'get_account_balance', None


def get_numerical_balance(task_configs):
    balance_response = get_account_balance(task_configs)
    return float(balance_response['AvailableBalance'])


def print_balance(task_configs):
    balance = get_numerical_balance(task_configs)
    print(f'Account balance is: ${balance:.{2}f}')


def expected_cost(data, task_configs):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :param task_configs:
    :return: cost if sufficient funds, false if not
    """
    hit_params = task_configs['hit_params']
    base_cost = float(hit_params['Reward'])
    n_assignments_per_hit = hit_params['MaxAssignments']
    min_fee_per_assignment = 0.01
    fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
    fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
    cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
    current_balance = get_numerical_balance(task_configs)
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: batch will cost ${cost_plus_fee:.{2}f}',
            f'and only ${current_balance:.{2}f} available.',
            f'An additional ${cost_plus_fee - current_balance:.{2}f} is needed.'
        )
        return False
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')
    return cost_plus_fee
