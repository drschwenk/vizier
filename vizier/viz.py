import numpy as np
import matplotlib.pylab as plt


def make_standard_fig(fig_labels=None, save=False, outfile='fig.png', label_color='0.25'):
    plt.rcParams['grid.linewidth'] = 0
    plt.rcParams['figure.figsize'] = (16.0, 10.0)
    if fig_labels:
        plt.title(fig_labels.get('fig_title', ''), fontsize=15, verticalalignment='bottom', color=label_color)
        plt.ylabel(fig_labels.get('y_label', ''), fontsize=15, labelpad=10, color=label_color)
        plt.xlabel(fig_labels.get('x_label', ''), fontsize=12, labelpad=10, color=label_color)
    plt.xticks(rotation=45)
    plt.tick_params(axis='x', which='major', labelsize=10)
    plt.tick_params(axis='y', which='major', labelsize=10)
    plt.tight_layout()
    if save:
        plt.savefig(outfile, bbox_inches='tight')
    plt.show()


def center_bin_labels(bins, **kwargs):
    bin_w = (bins.max() - bins.min()) / (len(bins) - 1)
    plt.xticks(np.arange(bins.min() + bin_w / 2, bins.max(), bin_w), bins, **kwargs)
    plt.xlim(bins[0], bins[-1])


def hit_status_counts(hit_stats):
    fig_labels = {
        'fig_title': 'HIT Statuses',
        'x_label': '# HITs',
        'y_label': 'Status',
    }
    hit_stats.value_counts().plot(kind='bar')
    make_standard_fig(fig_labels)


def worker_rate_hist(avg_rates, target_rate=10):
    fig_labels = {
        'fig_title': 'Worker Rates',
        'x_label': 'Hourly rate (USD)',
        'y_label': '# Workers',
    }
    bins = np.arange(0, 22)
    avg_rates.plot(kind='hist', bins=bins)
    center_bin_labels(bins, fontsize=20)
    plt.axvline(x=target_rate + 0.5, color='r', linewidth=3, linestyle='--')
    plt.legend(['Target Rate'], fontsize=12)
    make_standard_fig(fig_labels, save=True)
