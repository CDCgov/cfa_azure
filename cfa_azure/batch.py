import uuid


class Task:
    """
    Task object used with clients.AzureClient.run_dag()

    Attributes:
       cmd: docker command for task
       id: optional name for id
       dep: dependent Task(s)
    """

    def __init__(
        self, cmd: str, id: str | None = None, dep: str | list | None = None
    ):
        """
        Args:
            cmd (str): command to be used with Azure Batch task
            id (str, optional): optional id to identity tasks. Defaults to None.
            dep (str | list[str], optional): Task object(s) this task depends on. Defaults to None.
        """
        self.cmd = cmd
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        if isinstance(dep, list):
            self.deps = dep
        elif dep is None:
            self.deps = []
        else:
            self.deps = [dep]

    def __repr__(self):
        return self.id

    def before(self, other):
        """
        Set that this task needs to occur before another task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.before(t2) sets t1 must occure before t2.

        Args:
            other (Task): batch.Task object
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if self not in task.deps:
                task.deps.append(self)

    def after(self, other):
        """
        Set that this task needs to occur after another task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.after(t2) sets t1 must occur after t2.

        Args:
            other (Task): batch.Task object
        """
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if task not in self.deps:
                self.deps.append(task)

    def set_downstream(self, other):
        """
        Sets the downstream task from the current task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.set_downstream(t2) sets t2 as the downstream task from t1, like t1 >> t2

        Args:
            other (Task): batch.Task object
        """
        self.before(other)

    def set_upstream(self, other):
        """
        Sets the upstream task from the current task.

        Example:
            t1 = Task("some command")
            t2 = Task("another command")
            t1.set_upstream(t2) sets t2 as the upstream task from t1, like t1 << t2

        Args:
            other (Task): batch.Task object
        """
        self.after(other)
