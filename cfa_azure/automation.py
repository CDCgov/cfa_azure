import toml
import itertools
import pandas as pd
from cfa_azure.clients import AzureClient
from cfa_azure import helpers

def run_experiment(exp_config: str, auth_config: str):
    """Run jobs and tasks automatically based on the provided experiment config.

    exp_config (str): path to experiment config file (toml)
    auth_config (str): path to authorization config file (toml)
    """

    #read files
    exp_toml = toml.load(exp_config)
    if 'credential_method' in exp_toml['setup'].keys():
        credential_method = exp_toml['setup']['credential_method']
    else:
        credential_method = 'identity'
    if 'use_env_vars' in exp_toml['setup'].keys():
        use_env_vars = exp_toml['setup']['use_env_vars']
    else:
        use_env_vars = False

    try:
        client = AzureClient(
            config_path=auth_config,
            credential_method = credential_method,
            use_env_vars=use_env_vars)
    except Exception:
        print("could not create AzureClient object.")
        return None

    #check pool included in exp_toml and exists in azure
    if 'pool_name' in exp_toml['setup'].keys():
        if not helpers.check_pool_exists(resource_group_name= client.resource_group_name,
            account_name=client.account_name,
            pool_name = exp_toml['setup']['pool_name'],
            batch_mgmt_client=client.batch_mgmt_client):
            print(f"pool name {exp_toml['setup']['pool_name']} does not exist in the Azure environment.")
            return None
        pool_name = exp_toml['setup']['pool_name']
    else:
        print("could not find 'pool_name' key in 'setup' section of exp toml.")
        print("please specify a pool name to use.")
        return None

    #upload files if the section exists
    if 'upload' in exp_toml.keys():
        container_name = exp_toml['upload']['container_name']
        if 'location_in_blob' in exp_toml['upload'].keys():
            location_in_blob = exp_toml['upload']['location_in_blob']
        else:
            location_in_blob = ""
        if 'folders' in exp_toml['upload'].keys():
            client.upload_files_in_folder(folder_names=exp_toml['upload']['folders'],
                                          location_in_blob=location_in_blob,
                                          container_name=container_name)
        if 'files' in exp_toml['upload'].keys():
            client.upload_files(files=exp_toml['upload']['files'],
                                location_in_blob=location_in_blob,
                                container_name=container_name)

    #create the job
    job_id = exp_toml['job']['name']
    if 'save_logs_to_blob' in exp_toml['job'].keys():
        save_logs_to_blob = exp_toml['job']['save_logs_to_blob']
    else:
        save_logs_to_blob = None
    if 'logs_folder' in exp_toml['job'].keys():
        logs_folder = exp_toml['job']['logs_folder']
    else:
        logs_folder = None
    if 'task_retries' in exp_toml['job'].keys():
        task_retries = exp_toml['job']['task_retries']
    else:
        task_retries = 0
        
    client.add_job(job_id = job_id,
                   pool_name = pool_name,
                   save_logs_to_blob=save_logs_to_blob,
                   logs_folder=logs_folder,
                   task_retries=task_retries)

    #create the tasks for the experiment
    #get the container to use if necessary
    if 'container' in exp_toml['job'].keys():
        container = exp_toml['job']['container']
    else:
        container = None

    #submit the experiment tasks
    ex = exp_toml['experiment']
    var_list = [key for key in ex.keys() if key != "base_cmd"]
    var_values=[]
    for var in var_list:
        var_values.append(ex[var])

    v_v = list(itertools.product(*var_values))

    for params in v_v:
        client.add_task(job_id = job_id,
            docker_cmd = ex['base_cmd'].format(*params),
            container = container
        )

    if 'monitor_job' in exp_toml['job'].keys():
        if exp_toml['job']['monitor_job'] is True:
            client.monitor_job(job_id)


def run_tasks(task_config: str, auth_config: str):
    """Run jobs and tasks automatically based on the provided task config.

    task_config (str): path to task config file (toml)
    auth_config (str): path to authorization config file (toml)
    """

    #read files
    task_toml = toml.load(task_config)
    if 'credential_method' in task_toml['setup'].keys():
        credential_method = task_toml['setup']['credential_method']
    else:
        credential_method = 'identity'
    if 'use_env_vars' in task_toml['setup'].keys():
        use_env_vars = task_toml['setup']['use_env_vars']
    else:
        use_env_vars = False

    try:
        client = AzureClient(
            config_path=auth_config,
            credential_method = credential_method,
            use_env_vars=use_env_vars)
    except Exception:
        print("could not create AzureClient object.")
        return None

    #check pool included in task_toml and exists in azure
    if 'pool_name' in task_toml['setup'].keys():
        if not helpers.check_pool_exists(resource_group_name= client.resource_group_name,
            account_name=client.account_name,
            pool_name = task_toml['setup']['pool_name'],
            batch_mgmt_client=client.batch_mgmt_client):
            print(f"pool name {task_toml['setup']['pool_name']} does not exist in the Azure environment.")
            return None
        pool_name = task_toml['setup']['pool_name']
    else:
        print("could not find 'pool_name' key in 'setup' section of exp toml.")
        print("please specify a pool name to use.")
        return None

    #upload files if the section exists
    if 'upload' in task_toml.keys():
        container_name = task_toml['upload']['container_name']
        if 'location_in_blob' in task_toml['upload'].keys():
            location_in_blob = task_toml['upload']['location_in_blob']
        else:
            location_in_blob = ""
        if 'folders' in task_toml['upload'].keys():
            client.upload_files_in_folder(folder_names=task_toml['upload']['folders'],
                                          location_in_blob=location_in_blob,
                                          container_name=container_name)
        if 'files' in task_toml['upload'].keys():
            client.upload_files(files=task_toml['upload']['files'],
                                location_in_blob=location_in_blob,
                                container_name=container_name)

    #create the job
    job_id = task_toml['job']['name']
    if 'save_logs_to_blob' in task_toml['job'].keys():
        save_logs_to_blob = task_toml['job']['save_logs_to_blob']
    else:
        save_logs_to_blob = None
    if 'logs_folder' in task_toml['job'].keys():
        logs_folder = task_toml['job']['logs_folder']
    else:
        logs_folder = None
    if 'task_retries' in task_toml['job'].keys():
        task_retries = task_toml['job']['task_retries']
    else:
        task_retries = 0
        
    client.add_job(job_id = job_id,
                   pool_name = pool_name,
                   save_logs_to_blob=save_logs_to_blob,
                   logs_folder=logs_folder,
                   task_retries=task_retries)

    #create the tasks for the experiment
    #get the container to use if necessary
    if 'container' in task_toml['job'].keys():
        container = task_toml['job']['container']
    else:
        container = None

    #submit the tasks
    tt = task_toml['task']
    df = pd.json_normalize(tt)
    df.insert(0, "task_id", pd.Series("", index = range(df.shape[0])))
    #when kicking off a task we save the taskid to the row in df
    for i, item in enumerate(tt):
        if 'depends_on' in item.keys():
            d_list = []
            for d in item['depends_on']:
                d_task = df[df["name"]==d]["task_id"].values[0]
                d_list.append(d_task)
        else:
            d_list = None
        #check for other attributes
        if 'run_dependent_tasks_on_fail' in item.keys():
            run_dependent_tasks_on_fail = item['run_dependent_tasks_on_fail']
        else:
            run_dependent_tasks_on_fail = False
        #submit the task
        tid = client.add_task(
            job_id = job_id,
            docker_cmd = item['cmd'],
            depends_on = d_list,
            run_dependent_tasks_on_fail = run_dependent_tasks_on_fail,
            container = container
        )
        df.loc[i, 'task_id'] = tid


    if 'monitor_job' in task_toml['job'].keys():
        if task_toml['job']['monitor_job'] is True:
            client.monitor_job(job_id)
    return None