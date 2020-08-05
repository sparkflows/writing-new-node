from fire.workflowengine.workflow import Node


class NodeDataset(Node):

    path = ""

    def __init__(self, wf_id: int, name: str, description: str, path: str):
        super().__init__(wf_id, name, description)
        self.path = path
