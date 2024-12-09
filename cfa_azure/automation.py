import toml
import itertools
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