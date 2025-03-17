# DAGs with `cfa_azure`

A DAG is a graph containing directed edges but no cycles, ensuring no path leads back to the starting node. This is helpful in building complex workflows where order matters between certain tasks that are dependent on other tasks completing first.

To create a DAG using `cfa_azure`, the building blocks are Task objects from the `batch` module. Each task includes the docker command that will be run when the task executes in Azure Batch, and optionally an id you want to specify the task by, and any dependencies. Dependencies can be added at Task creation or at a later time, which will be discussed more below.

**Example to create Task objects**
```python
from cfa_azure.batch import Task
t1 = Task("python3 /main.py")
t2 = Task("python3 /second_task.py")
```

Once Task objects are created, dependencies can be added to the tasks via the following methods on a Task object:
- before()
- after()


For example, if a task `t1` needs to be run before task `t2`, the following could be run to set this dependency:
```python
t1.before(t2)
```

Similarly, if a task `t1` needs to be run after task `t2`, the following could be run to set this dependency:
```python
t1.after(t2)
```

Tasks can also be set to have multiple dependencies using multiple statements or a single list in one call. For example:
```python
#multiple statements
t1.after(t2)
t1.after(t3)

# or a list
t1.after([t2, t3])
```

Once all tasks have their task dependencies added, use the client method `run_dag()` to execute the DAG based on the provided tasks. The general structure of this method is comma-separated Task objects, followed by a `job_id` specification.

For example, if Tasks `t1`, `t2`, `t3`, and `t4` are to be run as DAG from the AzureClient object, do the following:
```python
client = AzureClient()
client.run_dag(t1, t2, t3, t4, job_id = "dag_job_example")
```

## A Simple Example
Say we have 5 tasks that we want to run in a certain order. Task 2 depends on Task 1, Task 3 needs to run before Task 4, and Task 5 needs to run after Task 3 and Task 4. The following end to end example could be used. Note that we use tasks t1-t5 for the Task names, but any names can be used.
```python
from cfa_azure.clients import AzureClient
from cfa_azure.batch import Task

client = AzureClient()

t1 = Task("dummy cmd 1")
t2 = Task("dummy cmd 2")
t3 = Task("dummy cmd 3")
t4 = Task("dummy cmd 4")
t5 = Task("dummy cmd 5")

t2.after(t1)
t3.before(t4)
t5.after([t3, t5])

client.add_job("dag_job_example")
client.run_dag(t1, t2, t3, t4, t5, job_id = "dag_job_example")
```

## Alternative Methods for Setting Dependencies
Besides the two ways of setting dependencies mentioned above (`before()` and `after()`), there are two additional methods keeping in line with Airflow DAGs. These two methods are `set_upstream()` and `set_downstream()` and are identical to the behavior of `before()` and `after()` as described, but some users may be more familiar with these methods if they have experience with Airflow. The methods are related as follows:
- before = set_downstream
- after = set_upstream

For example, a Task t1 that will be followed by Task t2 can be set by
```
t1.set_downstream(t2)
```
