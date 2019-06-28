# -*- coding: utf-8 -*-
""" Utilities

"""
import os
import time
from decorator import decorator


@decorator
def surface_hit_ids(action, *args, **kwargs):
    action_name, hits = action(*args, **kwargs)
    if hits and hits[0].get('HIT'):
        return action_name, [hit['HIT'] for hit in hits]
    return action_name, hits


def make_standard_fig(fig_labels=None, save=False, outfile='fig.png', label_color='0.25'):
    import matplotlib.pylab as plt
    plt.rcParams['grid.linewidth'] = 0
    plt.rcParams['figure.figsize'] = (16.0, 10.0)
    if fig_labels:
        if 'fig_title' in fig_labels:
            plt.title(fig_labels['fig_title'], fontsize=15, verticalalignment='bottom', color=label_color)
        if 'y_label' in fig_labels:
            plt.ylabel(fig_labels['y_label'], fontsize=15, labelpad=10, color=label_color)
        if 'x_label' in fig_labels:
            plt.xlabel(fig_labels['x_label'], fontsize=12, labelpad=10, color=label_color)
    plt.xticks(rotation=45)
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.tick_params(axis='y', which='major', labelsize=10)
    plt.tight_layout()
    plt.show()
    if save:
        plt.savefig(outfile, bbox_inches='tight')


def recall_template_args(**kwargs):
    """
    Collects all of the arguments expected by the interface template
    at HIT creation
    : return (set): the expected arguments
    """
    import re
    template_dir = kwargs['configuration']['interface_params']['template_dir']
    template_fn = kwargs['configuration']['interface_params']['template_file']
    template_fp = os.path.join(template_dir, template_fn)
    with open(template_fp) as file:
        template_html = file.read()
    template_args = re.findall(r'\{\{(.*?)\}', template_html)
    return set(template_args)


def _create_timestamp():
    _, month, day, clock, year, = time.asctime().lower().split()
    hour, minute, _ = clock.split(':')
    timestamp = '_'.join([year, month, day, hour, minute])
    return timestamp


def prepare_output_path(action, configs, include_timestamp=True):
    output_dir_base = configs['serialization_params']['output_dir_base']
    experiment = configs['experiment_params']['batch_id']
    out_dir = os.path.join(output_dir_base, experiment)
    os.makedirs(out_dir, exist_ok=True)
    action_name = 'result--' + action.__name__ if callable(action) else action
    environment = None if configs['amt_client_params']['in_production'] else 'sbx'
    timestamp = _create_timestamp() if include_timestamp else None
    out_fn_components = [environment, action_name, timestamp]
    file_name = '--'.join(list(filter(None, out_fn_components)))
    output_fp = os.path.join(out_dir, file_name)
    return output_fp


def confirm_action(prompt):
    while True:
        confirm = input(prompt)
        if confirm.lower() in ('y', 'n', 'yes', 'no'):
            if confirm[0] != 'y':
                import sys
                print('aborting...')
                sys.exit()
            break
        else:
            print("\n Invalid--Please enter y or n.")


