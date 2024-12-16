# Automation Module

## Overview
The `automation` module as part of `cfa_azure` is designed to perform certain actions in Azure based on a configuration file. This allows users to interact with Azure via this `cfa_azure` package even with little python experience. It also allows users to take their config file and make minor tweaks in order to upload new files, run different tasks, etc. It provides a more flexible framework than changing user-provided parameters spread throughout different functions in a python script.

Currently, the `automation` module is comprised of two functions to automate certain tasks:
1. `run_experiment`: useful when needing to create tasks based on a base command and a permutation of variables
2. `run_tasks`: useful when needing to create specific tasks, with dependencies allowed

## Setting up the config
There are two slightly different configuration files depending on which function you want to use. Both config files have similar structures. Each will have the following sections and keys:
- [setup]
    - credential_method: choices from sp, identity, or env for how to get Azure credentials. Optional. If omitted, manged identity will be used.
    - use_env_vars: choices of true or false, on whether to use environment variables instead of an auth config. Optional. If omitted, set to false.
    - pool_name: the name of the pool to use for jobs and tasks. Required.
- [upload] - optional, only include if folders/files need to be uploaded.
    - container_name: name of Blob container to use for the final destination
    - location_in_blob:  the folder structure of where to place the uploaded files in blob. Optional. If omitted, files will be placed in the root of the Blob.
    - folders: list of folders to upload to Blob. Optional.
    - files: list of files to upload to Blob. Optional.
- [job]
    - name: name of the job to create. Required.
    - save_logs_to_blob: name of Blob to save logs to, if desired. Optional. If omitted, logs will not be saved to Blob.
    - logs_folder: folder structure for where to save the logs in Blob. Optional.
    - task_retries: number of retries if a task fails. Optional. If omitted, defaults to 0.
    - monitor_job: whether to monitor the job in the terminal. Optional. If omitted, job will not be monitored.
    - container: full ACR container name to use for the job. Optional. If omitted, the container associated with the specified pool will be used.

The experiment config will have the following additional section and keys:
- [experiment]
    - base_cmd: the full docker command for executing the tasks, including some indication of how the variables should be included. This usually entails a flag or argument. See the example for more details.
    - the variable names along with their list of possible values. See the example for more details. Note that the order matters. For example, if you have variables `input` and `num` listed in that order in the config file, the values for `input` will be put into the {0} spot in the base_cmd and the values for `num` will be put into the {1} spot. Each variable should be on its own line in the form `var_name=[val1, val2, ...]`.

The task config will have the following additional section(s) and keys:
- [[task]]
    - cmd: the full docker command to use for the task
    - name: the name for the task. Required if using dependent tasks.
    - depends_on: a list of task names the task depends on to run. Optional.
    - run_dependent_tasks_on_fail: true or false, whether you want dependent tasks to run even if the parent task(s) fails. Optional.

Notice above that [[task]] is in double brackets. This is necessary because there can be repeated sections starting with [[task]] as the header, followed by the task-specific information; one [[task]] section for each task to be submitted. See the task_config.toml example for more information.

See the example [experiment config](examples/automation/exp_config.toml) and [task config](examples/automation/task_config.toml) for more help.

## run_experiment()

The `run_experiment` function is meant for applying all permutations of a set of variables to common base command. For example, if you have variables var1 and var2, where var1 can be values 10 or 11 and var2 can be 98 or 99, and you need to apply all combinations (really permutations) of these variables to a single command, this is the function to use. It would create 4 tasks with (var1, var2) values of (10, 98), (10, 99), (11, 98), and (11, 99), passed into the command as determined in the config file.

Here's a more concrete example. Suppose we have the following experiment section in the experiment config:
```
[experiment]
base_cmd = "python3 /input/data/vars.py --var1 {0} --var2 {1} --var3 {2}"
var1 = [1, 2, 4]
var2 = [10,11,12]
var3 = ['90', '99', '98']
```
The base command indicates a python script vars.py will be run with three command line arguments with flags called var1, var2, and var3. For simplicity, we also use the names var1, var2, var3 for setting the list of options to cycle through. Because var1 is listed first under base_cmd it will be passed into the {0} spot of the base_cmd, since var2 is next it is passed into the {1} spot, and so on. Any number of variables can be used and the number of elements of each list do not need to be equal. This experiment will generate 27 tasks, one for each permutation of [1, 2, 3], [10, 11, 12], ['90', '99', '98']. More specifically, the following commands will be used for the tasks:
```
python3 /input/data/vars.py --var1 1 --var2 10 --var3 '90'
python3 /input/data/vars.py --var1 1 --var2 10 --var3 '99'
python3 /input/data/vars.py --var1 1 --var2 10 --var3 '98'
python3 /input/data/vars.py --var1 2 --var2 11 --var3 '90'
python3 /input/data/vars.py --var1 2 --var2 11 --var3 '99'
...
...
```

You can use the `run_experiment` function in two lines of code, as shown below.
```
from cfa_azure.automation import run_experiment
run_experiment(exp_config = "path/to/exp_config.toml",
    auth_config = "path/to/auth_config.toml")
```


## run_tasks()
The `run_tasks()` function is designed to take an arbitrary number of tasks from a configuration file to submit them as part of a job. Any folders or files included in the [upload] section of the config will be uploaded before kicking off the tasks.

Suppose we want to kick off two tasks we'll call "do_first" and "second_depends_on_first", where the R script is stored in Blob storage at the relative mount path "input/scripts/execute.R", the script can take different flags as input,  the second task depends on the first, and we will run the second task even if the first fails. We would setup the task_config.toml to have the following info in the [[task]] sections:
```
[[task]]
cmd = 'Rscript /input/scripts/execute.R --data /example/data.csv"
name = "do_first"

[[task]]
cmd = 'Rscript /input/scripts/execute.R --model /example/model.pkl'
name = "second_depends_on_first"
depends_on = ["do_first"]
run_dependent_tasks_on_fail = true
```

You can then run the tasks in two lines of code, as shown below.
```
from cfa_azure.automation import run_tasks
run_experiment(task_config = "path/to/task_config.toml",
    auth_config = "path/to/auth_config.toml")
```