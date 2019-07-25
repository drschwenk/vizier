from pprint import pprint
from crowdsourcery.amt_client import amt_single_action


@amt_single_action
def get_account_balance(**kwargs):
    return 'get_account_balance', None


def get_numerical_balance():
    """
    Checks account balance of active profile
    :return (float): account balance
    """
    balance_response = get_account_balance()
    return float(balance_response['AvailableBalance'])


def print_balance():
    balance = get_numerical_balance()
    print(f'Account balance is: ${balance:.{2}f}')


def expected_cost(data, **kwargs):
    """
    Computes the expected cost of a hit batch
    To adjust for subtleties of the amt fees, see:
    www.mturk.com/pricing
    :param data: task data
    :return: cost if sufficient funds, false if not
    """
    hit_params = kwargs['configuration']['hit_params']
    base_cost = float(hit_params['Reward'])
    n_assignments_per_hit = hit_params['MaxAssignments']
    fee_percentage = 0.2 if n_assignments_per_hit < 10 else 0.4
    min_fee_per_assignment = 0.01
    fee_per_assignment = max(fee_percentage * base_cost, min_fee_per_assignment) + base_cost
    cost_plus_fee = round(n_assignments_per_hit * fee_per_assignment * len(data), 2)
    current_balance = get_numerical_balance()
    if cost_plus_fee > current_balance:
        print(
            f'Insufficient funds: batch will cost ${cost_plus_fee:.{2}f}',
            f'and only ${current_balance:.{2}f} available.',
            f'An additional ${cost_plus_fee - current_balance:.{2}f} is needed.'
        )
    print(f'Batch will cost ${cost_plus_fee:.{2}f}')


def summarize_proposed_task(data, **kwargs):

    def add_space():
        for i in range(2):
            print('.')

    def add_section_break():
        print('-'.join([''] * 100))

    params_to_display = [
        'experiment_params',
        'hit_params',
        'amt_client_params'
    ]
    add_section_break()
    for param_type in params_to_display:
        print(param_type, ':')
        pprint(kwargs['configuration'][param_type])
        add_section_break()
    add_space()
    expected_cost(data, **kwargs)
    add_space()
    print_balance()
    add_space()
    in_prod = kwargs['configuration']['amt_client_params']['in_production']
    amt_environment = 'production' if in_prod else 'sandbox'
    print(f'task will be launched in {amt_environment.upper()}')
    add_space()

