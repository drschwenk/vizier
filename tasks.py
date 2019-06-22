from invoke import Collection, task
import subprocess
from vizier import utils
from vizier import create_hits
from vizier import manage_hits
from vizier import config
import warnings


@task
def _setup_task_config(ctx):
    config_file = ctx.get('task_config_fp', None)
    config_fp_used = config.set_input_file_path(config_file)
    utils.logger.info('using %s for task configuration', config_fp_used)


def confirm_action(prompt):
    while True:
        confirm = input(prompt)
        if confirm.lower() in ('y', 'n', 'yes', 'no'):
            if 'y' != confirm[0]:
                import sys
                print('aborting...')
                sys.exit()
            break
        else:
            print("\n Invalid--Please enter y or n.")


@task(pre=[_setup_task_config])
def preview_hit_interface(ctx, data_fp, data_idx=None, open_in_browser=False):
    data = utils.load_input_data(data_fp)
    if data_idx:
        datum = data[data_idx]
    else:
        import random
        datum = random.choice(data)
    out_fp = create_hits.preview_hit_interface(datum)
    if open_in_browser:
        subprocess.call(['open', out_fp])


@task(pre=[_setup_task_config])
def create_hit_group(ctx, data_fp):
    data = utils.load_input_data(data_fp)
    utils.summarize_proposed_task(data)
    confirm_action('launch task with these settings? y/n\n')
    confirm_action(f'create {len(data)} hits? y/n\n')
    create_hits.create_hit_group(data[:1])


@task(pre=[_setup_task_config])
def get_hit_statuses(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    hit_stats = manage_hits.get_hit_statuses(hits)
    print(hit_stats.value_counts())


@task(pre=[_setup_task_config])
def get_assignments(ctx, hit_group_fp, extract=False):
    hits = utils.deserialize_result(hit_group_fp)
    if extract:
        assignment_results = manage_hits.get_and_extract_results(hits)
    else:
        assignment_results = manage_hits.get_assignments(hits)
    if not assignment_results:
        print('no results')


@task(pre=[_setup_task_config])
def approve_assignments(ctx, assignment_group_fp):
    assignments = utils.deserialize_result(assignment_group_fp)
    manage_hits.approve_assignments(assignments)


@task(pre=[_setup_task_config])
def approve_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    manage_hits.approve_hits(hits)


@task(pre=[_setup_task_config])
def get_all_hits(ctx):
    all_hits = manage_hits.get_all_hits()
    utils.serialize_result(all_hits, 'json', './all_profile_hits.json')


@task(pre=[_setup_task_config])
def expire_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    manage_hits.expire_hits(hits)


@task(pre=[_setup_task_config])
def delete_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    manage_hits.delete_hits(hits)


@task(pre=[_setup_task_config])
def force_delete_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    manage_hits.force_delete_hits(hits)


# namespace = Collection()
# namespace.add_collection(Collection.from_module(create_hits))
# namespace.add_task(test_prev)
# inner.configure()
