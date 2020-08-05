from fire.workflowengine.workflow import *
from fire.output.output import Output
from fire.output import *


class OutputGraph(Output):
    def __init__(self, id: int, name: str, title: str, y: [[]], resultType: int, visibility: str):

        self.y = y

        super().__init__(id, name, title, "graph", resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    def __str__(self):
        return str(self.id) + " : " + self.name + " : " + self.title + " : " + self.type

    @staticmethod
    def load(d: dict):
        return OutputTable(**d)

    def toJSON(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\\\"")

        return tempStr

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr


