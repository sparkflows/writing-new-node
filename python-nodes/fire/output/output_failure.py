from fire.workflowengine.workflow import *
from fire.output.output import Output


class OutputFailure(Output):
    def __init__(self, id: int, name: str, title: str, text: str, resultType: int, visibility: str):

        self.text = text

        super().__init__(id, name, title, "failure", resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputHeader(**d)

    def toJSON1(self):

        # replace ' with : Error : No module named 'keras'
        self.text = self.text.replace("\'", ":")

        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
