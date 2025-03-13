import uuid


class Task:
    def __init__(self, cmd, id=None, dep=None):
        """
        __init__ _summary_

        Args:
            cmd (_type_): _description_
            id (_type_, optional): _description_. Defaults to None.
            dep (_type_, optional): _description_. Defaults to None.
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
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if self not in task.deps:
                task.deps.append(self)

    def after(self, other):
        if not isinstance(other, list):
            other = [other]
        for task in other:
            if task not in self.deps:
                self.deps.append(task)

    def set_downstream(self, other):
        self.before(other)

    def set_upstream(self, other):
        self.after(other)
