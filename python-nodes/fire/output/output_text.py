from fire.workflowengine.workflow import *
from fire.output.output import Output


class OutputText(Output):
    def __init__(self, id: int, name: str, title: str, text: str, resultType: int, visibility: str):
        self.text = text

        super().__init__(id, name, title, "text", resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputText(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr



