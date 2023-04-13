import sys
import warnings

import numpy as np
import yaml
from loguru import logger
from sklearn.linear_model import LinearRegression

from . import parser, database, runner, get_task

# %%
job_config = {}
algorithm_config = {}
trace_config = {}


def _get_task(trace_file, cache_type, cache_size, parameters, n_early_stop, memory_window):
    task = {
        **parameters,
        'trace_file': trace_file,
        'cache_type': cache_type,
        'cache_size': cache_size,
        'n_early_stop': n_early_stop,
        'memory_window': memory_window,
    }
    return task


def get_validation_tasks_per_cache_size(trace_file, cache_type, cache_size, parameters, n_dev):
    global df
    n_memory_window_search_per_cache_size = job_config['n_memory_window_search_per_cache_size']
    # try each window
    tasks = []
    if 'byte_million_req' not in parameters:
        warnings.warn("no byte_million_req info, estimate memory window lower bound as 4k")
        memory_window_lower_bound = 4096
    else:
        if 'uni_size' in parameters and parameters['uni_size'] == 1:
            byte_million_req = 1000000
        else:
            byte_million_req = parameters['byte_million_req']
        n_req_one_cache_size = int(cache_size / byte_million_req * 1e6)
        memory_window_lower_bound = n_req_one_cache_size
    memory_windows = np.logspace(np.log10(memory_window_lower_bound), np.log10(0.4 * n_dev), n_memory_window_search_per_cache_size).astype(np.int64)
    memory_windows_to_validate = []
    for memory_window in memory_windows:
        if len(df) == 0 or len(df[(df['cache_size'] == cache_size) & (df['memory_window'] == memory_window)]) == 0:
            # override n_early stop
            task = _get_task(trace_file, cache_type, cache_size, parameters, n_dev, memory_window)
            tasks.append(task)
            memory_windows_to_validate.append(memory_window)
        else:
            # there should be unique config
            assert len(df[(df['cache_size'] == cache_size) & (df['memory_window'] == memory_window)]) == 1
    logger.info(f'For {trace_file}/size={cache_size}/{cache_type}, memory windows to validate: {memory_windows_to_validate}',
          file=sys.stderr)
    return tasks


def check_need_fitting(cache_size):
    global df
    df1 = df[df['cache_size'] == cache_size]
    candidate = df1['memory_window'].iloc[0]
    # You could try different memory window setting to see if you can find a U-curve: y-axis is the byte miss ratio
    # and X-axis is the memory window. Only when the algorithm is confident about such U curve happens, it will select
    # that candidate The "if candidate .." line is to select that. If there is no such U-curve, no memory window will
    # be selected at current cache size
    if candidate == df1['memory_window'].max():
        # we don't accept the memory window at right boundary, as it may not be a local minimum
        return True
    else:
        return False


def get_fitting_task(trace_file, cache_type, cache_size, parameters, n_dev):
    global df
    n_fitting_points = job_config['n_fitting_cache_size']
    current_cache_size = cache_size // 2
    xs = []
    ys = []
    while len(xs) != n_fitting_points:
        tasks = get_validation_tasks_per_cache_size(trace_file, cache_type, current_cache_size, parameters, n_dev)
        if len(tasks) == 0:
            need_fitting = check_need_fitting(current_cache_size)
            if need_fitting == False:
                window = df[df['cache_size'] == current_cache_size]['memory_window'].iloc[0]
                xs.append(current_cache_size)
                ys.append(window)
            # recursively add a smaller cache size
            current_cache_size = current_cache_size // 2
        else:
            return tasks
    reg = LinearRegression(fit_intercept=False).fit(np.array(xs).reshape(-1, 1), np.array(ys))
    memory_window = int(reg.predict([[cache_size]])[0])
    print(f'find best memory window for {trace_file} cache size {cache_size}: {memory_window}')
    return []


def get_tasks_per_cache_size(trace_file, cache_type, cache_size, parameters):
    global df
    # do a db read for dev tasks
    n_req = parameters['n_req']
    n_dev = int(n_req * job_config['ratio_dev']) if parameters['n_early_stop'] == -1 else int(
        parameters['n_early_stop'] * job_config['ratio_dev'])
    n_warmup = int(0.8 * n_dev)
    db_keys = {}
    for k, v in parameters.items():
        if k not in {
            'dburi',
            'dbcollection',
            'enable_trace_format_check',
            'n_early_stop',
        }:
            db_keys[k] = str(v)
    df = database.load_reports(
        trace_file=trace_file,
        cache_type=cache_type,
        # no cache size because may check smaller cache sizes
        # cache_size=cache_size,
        n_early_stop=str(n_dev),
        n_warmup=n_warmup,
        dburi=parameters["dburi"],
        dbcollection=parameters["dbcollection"],
        **db_keys
    )
    tasks = get_validation_tasks_per_cache_size(trace_file, cache_type, cache_size, parameters, n_dev)
    if len(tasks) != 0:
        return tasks
    need_fitting = check_need_fitting(cache_size)
    if need_fitting is False:
        # if not need fitting, then select the window with smallest bmr
        memory_window = df[df['cache_size'] == cache_size]['memory_window'].iloc[0]
        print(f'find best memory window for {trace_file} cache size {cache_size}: {memory_window}')
        return []
    return get_fitting_task(trace_file, cache_type, cache_size, parameters, n_dev)


def get_cache_size_and_parameter_list(trace_file, cache_type, cache_size_or_size_parameters):
    # element can be k: v or k: list[v], which would be expanded with cartesian product
    # priority: global < default < per trace < per trace per algorithm < per trace per algorithm per cache size
    parameters = {}
    # global
    for k, v in job_config.items():
        if k not in [
            'cache_types',
            'trace_files',
            'algorithm_param_file',
            'trace_param_file',
            'job_file',
            'nodes',
            'n_memory_window_search_per_cache_size',
            'n_fitting_cache_size',
            'ratio_dev',
        ]:
            parameters[k] = v
    # default
    if cache_type in algorithm_config:
        for k, v in algorithm_config[cache_type].items():
            if k == 'memory_window':
                logger.info(f'{trace_file} {cache_type} ignore config: {k}={v}', file=sys.stderr)
            else:
                parameters[k] = v
    # per trace
    for k, v in trace_config[trace_file].items():
        if k not in ['cache_sizes'] and k not in trace_config:
            parameters[k] = v
    # per trace per algorithm
    if cache_type in trace_config[trace_file]:
        for k, v in trace_config[trace_file][cache_type].items():
            if k == 'memory_window':
                logger.info(f'{trace_file} {cache_type} ignore config: {k}={v}', file=sys.stderr)
            else:
                parameters[k] = v
    # per trace per algorithm per cache size
    if isinstance(cache_size_or_size_parameters, dict):
        # only 1 key (single cache size) is allowed
        assert (len(cache_size_or_size_parameters) == 1)
        cache_size = list(cache_size_or_size_parameters.keys())[0]
        if cache_type in cache_size_or_size_parameters[cache_size]:
            # per cache size parameters overwrite other parameters
            for k, v in cache_size_or_size_parameters[cache_size][cache_type].items():
                if k == 'memory_window':
                    logger.info(f'{trace_file} size={cache_size} {cache_type} ignore config: {k}={v}', file=sys.stderr)
                else:
                    parameters[k] = v
    else:
        cache_size = cache_size_or_size_parameters
    parameter_list = get_task.cartesian_product(parameters)
    return cache_size, parameter_list


def get_tasks():
    """
     convert job config to list of task
     @:returns dict/[dict]
     """
    # current version is only for LRB
    if job_config['cache_types'] != ['LRB']:
        raise ValueError(f'Error: cache_type={job_config["cache_types"]}, but only supports LRB')

    tasks = []
    for trace_file in job_config['trace_files']:
        for cache_type in job_config['cache_types']:
            for cache_size_or_size_parameters in trace_config[trace_file]['cache_sizes']:
                cache_size, parameter_list = get_cache_size_and_parameter_list(trace_file, cache_type,
                                                                               cache_size_or_size_parameters)
                for parameters in parameter_list:
                    # user cannot specify the memory_window parameter, the program need to compute it
                    assert 'memory_window' not in parameters
                    _tasks = get_tasks_per_cache_size(trace_file, cache_type, cache_size, parameters)
                    tasks.extend(_tasks)
    # deduplicate tasks
    tasks = [dict(t) for t in {tuple(d.items()) for d in tasks}]
    return tasks


def main():
    global job_config, algorithm_config, trace_config
    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and sys.version_info[1] < 6):
        raise Exception('Error: python version need to be at least 3.6')
    args = parser.parse_cmd_args_lrb_window_search()
    with open(args['job_file']) as f:
        job_config = yaml.load(f, Loader=yaml.FullLoader)
    job_config['dburi'] = args['dburi']
    job_config['n_memory_window_search_per_cache_size'] = args['n_memory_window_search_per_cache_size']
    job_config['n_fitting_cache_size'] = args['n_fitting_cache_size']
    job_config['ratio_dev'] = args['ratio_dev']
    with open(args['algorithm_param_file']) as f:
        algorithm_config = yaml.load(f, Loader=yaml.FullLoader)
    with open(args['trace_param_file']) as f:
        trace_config = yaml.load(f, Loader=yaml.FullLoader)

    while True:
        tasks = get_tasks()
        if len(tasks) == 0:
            break
        runner.run(job_config, tasks)


if __name__ == '__main__':
    main()
