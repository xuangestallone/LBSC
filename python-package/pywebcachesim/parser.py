import argparse


def get_parser():
    # how to schedule parallel simulations
    parser = argparse.ArgumentParser()
    parser.add_argument('job_file', type=str, help='job config file')
    parser.add_argument('algorithm_param_file', type=str, help='algorithm parameter config file')
    parser.add_argument('trace_param_file', type=str, help='trace parameter config file')
    parser.add_argument('dburi', type=str, help='mongodb database')
    return parser


def get_lrb_window_search_parser():
    parser = get_parser()
    parser.add_argument('--n_memory_window_search_per_cache_size', type=int, default=16,
                        help='for each cache search, how many different memory windows to search')
    parser.add_argument('--n_fitting_cache_size', type=int, default=2,
                        help='if the cache size is too large to directly estimate memory window, how many smaller '
                             'cache sizes to use for fitting')
    parser.add_argument('--ratio_dev', type=float, default=0.2,
                        help='ratio for the dev prefix')
    return parser


def parse_cmd_args():
    parser = get_parser()
    args = parser.parse_args()
    return vars(args)


def parse_cmd_args_lrb_window_search():
    parser = get_lrb_window_search_parser()
    args = parser.parse_args()
    return vars(args)
