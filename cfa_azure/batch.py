import uuid

class Task():
    def __init__(self, cmd, id = None, dep=[]):
        self.cmd = cmd
        if id is None:
            self.id = str(uuid.uuid4())
        else:
            self.id = id
        if isinstance(dep, list):
            self.deps = dep
        else:
            self.deps = [dep]
    def __repr__(self):
        return self.id