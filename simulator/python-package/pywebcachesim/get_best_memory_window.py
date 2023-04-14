import sys
from pywebcachesim import parser, database, runner, get_task
import yaml
from emukit.examples.gp_bayesian_optimization.single_objective_bayesian_optimization import GPBayesianOptimization
from emukit.core import ParameterSpace, ContinuousParameter
from copy import deepcopy
import numpy as np


def update_by_file(args, filename):
    args_cp = deepcopy(args)
    # job config file
    with open(filename) as f:
        file_params = yaml.load(f, Loader=yaml.FullLoader)
    for k, v in file_params.items():
        if args_cp.get(k) is None:
            args_cp[k] = v
    return args_cp


def get_next_memory_windows(trace_file, cache_type, cache_size, n_early_stop, seq_start, parameters, args_global):
    # use 80% warmup, manually check for unimodal behavior
    n_warmup = int(0.8 * n_early_stop)
    # per cache size parameters overwrite other parameters
    df = database.load_reports(
        trace_file=trace_file,
        cache_type=cache_type,
        cache_size=str(cache_size),
        seq_start=str(seq_start),
        n_early_stop=str(n_early_stop),
        n_warmup=n_warmup,
        version=parameters['version'],  # use version as a strong key
        dburi=parameters["dburi"],
        dbcollection=parameters["dbcollection"],
    )
    if len(df) == 0:
        next_windows = np.linspace(1, int(0.4 * n_early_stop), args_global['n_beam']+1, dtype=int)[1:]
    else:
        # window at most 40% of length
        parameter_space = ParameterSpace([ContinuousParameter('x', 1, int(0.4 * n_early_stop))])
        bo = GPBayesianOptimization(variables_list=parameter_space.parameters,
                                    X=df['memory_window'].values.reshape(-1, 1),
                                    Y=df['byte_miss_ratio'].values.reshape(-1, 1),
                                    batch_size=args_global['n_beam'])
        next_windows = bo.suggest_new_locations().reshape(-1).astype(int)
    return next_windows


def get_cache_size_and_parameter_list(trace_file, cache_type, cache_size_or_size_parameters, args_size, args_global):
    # element can be k: v or k: list[v], which would be expanded with cartesian product
    # priority: global < default < per trace < per trace per algorithm < per trace per algorithm per cache size
    parameters = {}
    # global
    for k, v in args_global.items():
        if k not in [
            'cache_types',
            'trace_files',
            'algorithm_param_file',
            'authentication_file',
            'trace_param_file',
            'job_file',
            'nodes',
            'n_iteration',
            'n_beam',
        ] and v is not None:
            parameters[k] = v
    # default
    default_algorithm_params = update_by_file({}, args_size['algorithm_param_file'])
    if cache_type in default_algorithm_params:
        parameters = {**parameters, **default_algorithm_params[cache_type]}
    # per trace
    for k, v in args_size[trace_file].items():
        if k not in ['cache_sizes'] and k not in default_algorithm_params and v is not None:
            parameters[k] = v
    # per trace per algorithm
    if cache_type in args_size[trace_file]:
        # trace parameters overwrite default parameters
        parameters = {**parameters, **args_size[trace_file][cache_type]}
    # per trace per algorithm per cache size
    if isinstance(cache_size_or_size_parameters, dict):
        # only 1 key (single cache size) is allowed
        assert (len(cache_size_or_size_parameters) == 1)
        cache_size = list(cache_size_or_size_parameters.keys())[0]
        if cache_type in cache_size_or_size_parameters[cache_size]:
            # per cache size parameters overwrite other parameters
            parameters = {**parameters, **cache_size_or_size_parameters[cache_size][cache_type]}
    else:
        cache_size = cache_size_or_size_parameters
    parameter_list = get_task.cartesian_product(parameters)
    return cache_size, parameter_list


def get_window_search_task(args):
    """
     convert job config to list of task
     @:returns dict/[dict]
     """
    # not modify input argument args
    args = update_by_file(args, args['authentication_file'])
    # current version is only for LRB
    assert args['cache_types'] == ['LRB']

    tasks = []
    for trace_file in args['trace_files']:
        for cache_type in args['cache_types']:
            args_csize = update_by_file(args, args['trace_param_file'])
            for cache_size_or_size_parameters in args_csize[trace_file]['cache_sizes']:
                cache_size, parameter_list = get_cache_size_and_parameter_list(trace_file, cache_type,
                                                                       cache_size_or_size_parameters, args_csize, args)
                for parameters in parameter_list:
                    assert 'memory_window' not in parameters
                    n_req = parameters['n_req']
                    n_early_stop = parameters['n_early_stop']
                    # don't handle partial segment
                    for seq_start in range(0, n_req//n_early_stop*n_early_stop, n_early_stop):
                        new_memory_windows = get_next_memory_windows(trace_file, cache_type, cache_size, n_early_stop,
                                                                     seq_start, parameters, args)
                        for memory_window in new_memory_windows:
                            task = {
                                'trace_file': trace_file,
                                'cache_type': cache_type,
                                'cache_size': cache_size,
                                'memory_window': int(memory_window),
                                'seq_start': seq_start,
                                **parameters,
                            }
                            tasks.append(task)
    return tasks


def main():
    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 6):
        raise Exception('Error: python version need to be at least 3.6')
    args = parser.parse_cmd_args()
    args = update_by_file(args, args['job_file'])

    for _ in range(args['n_iteration']):
        tasks = get_window_search_task(args)
        runner.run(args, tasks)


if __name__ == '__main__':
    main()
