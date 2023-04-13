import subprocess
import time


def to_task_str(task: dict):
    """
    split deterministic args and nodeterminstics args. Add _ prefix to later
    """

    params = {}
    for k, v in task.items():
        if k not in ['trace_file', 'cache_type', 'cache_size'] and v is not None:
            params[k] = str(v)
    task_id = str(int(time.time() * 1000000))
    # use timestamp as task id
    params['task_id'] = task_id
    params = [f'--{k}={v}'for k, v in params.items()]
    params = ' '.join(params)
    res = f'$WEBCACHESIM_ROOT/build/bin/webcachesim_cli {task["trace_file"]} {task["cache_type"]} {task["cache_size"]} {params}'
    return task_id, res


def run(job_config: dict, tasks: list):
    ts = int(time.time())
    print(f'n_task: {len(tasks)}\n '
          f'generating job file to /tmp/{ts}.job')
    with open(f'/tmp/{ts}.job', 'w') as f:
        for i, task in enumerate(tasks):
            task_id, task_str = to_task_str(task)
            # f.write(task_str+f' &> /tmp/{ts}.log\n')
            task_str = f'bash --login -c "{task_str}" &> /tmp/{task_id}.log\n'
            if i == 0:
                print(f'first task: {task_str}')
            f.write(task_str)
    with open(f'/tmp/{ts}.job') as f:
        command = ['parallel', '-v', '--eta', '--shuf', '--sshdelay', '0.1']
        for n in job_config['nodes']:
            command.extend(['-S', n])
        print(f"{' '.join(command)} < /tmp/{ts}.job")
        subprocess.run(command,
                       stdin=f)




