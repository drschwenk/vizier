from invoke import task
from vizier import config
from vizier import utils
from vizier import storage
from vizier import creation
from vizier import management


@task
def _set_config(ctx):
    config_file = ctx.get('task_config_fp', None)
    config_fp_used = config.set_input_file_path(config_file)
    from vizier.log import logger
    logger.debug('using %s for task configuration', config_fp_used)


@task(pre=[_set_config])
def preview_interface(ctx, input_data_fp, data_idx=None, open_in_browser=False):
    data = utils.load_input_data(input_data_fp)
    if data_idx:
        datum = data[data_idx]
    else:
        import random
        datum = random.choice(data)
    out_fp = creation.preview_interface(datum)
    if open_in_browser:
        import subprocess
        subprocess.call(['open', out_fp])


@task(pre=[_set_config])
def create_hits(ctx, input_data_fp):
    data = utils.load_input_data(input_data_fp)
    creation.create_hits(data[:10])


@task(pre=[_set_config])
def get_hit_status(ctx, hit_group_fp, plot=False):
    hits = utils.deserialize_result(hit_group_fp)
    hit_stats = management.get_hit_statuses(hits)
    if plot:
        hit_stats.value_counts().plot(kind='bar')
        fig_labels = {
            'fig_title': 'HIT Statuses',
            'x_label': '# HITs',
            'y_label': 'Status',
        }
        utils.make_standard_fig(fig_labels)
    else:
        print(hit_stats.value_counts())


@task(pre=[_set_config])
def get_assignments(ctx, hit_group_fp, extract=False):
    hits = utils.deserialize_result(hit_group_fp)
    if extract:
        assignment_results = management.get_and_extract_results(hits)
    else:
        assignment_results = management.get_assignments(hits)
    if not assignment_results:
        print('no results')


@task(pre=[_set_config])
def approve_assignments(ctx, assignment_group_fp):
    assignments = utils.deserialize_result(assignment_group_fp)
    management.approve_assignments(assignments)


@task(pre=[_set_config])
def approve_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    management.approve_hits(hits)


@task(pre=[_set_config])
def get_all_hits(ctx, out_file=None):
    if not out_file:
        out_file = './all_profile_hits.json'
    all_hits = management.get_all_hits()
    utils.serialize_result(all_hits, 'json', out_file)


@task(pre=[_set_config])
def change_hit_review_status(ctx, hit_group_fp, revert=False):
    hits = utils.deserialize_result(hit_group_fp)
    management.change_hit_review_status(hits, revert=revert)


@task(pre=[_set_config])
def expire_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    management.expire_hits(hits)


@task(pre=[_set_config])
def delete_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    management.delete_hits(hits)


@task(pre=[_set_config])
def force_delete_hits(ctx, hit_group_fp):
    hits = utils.deserialize_result(hit_group_fp)
    management.force_delete_hits(hits)


@task(pre=[_set_config])
def upload_to_s3(ctx, file_path):
    storage.upload_object(file_path)


@task(pre=[_set_config])
def list_working_s3_folder(ctx, display_metadata=False):
    storage.list_working_folder(display_metadata)
