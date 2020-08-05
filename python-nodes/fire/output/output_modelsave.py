from fire.output.output import Output


class OutputModelSave(Output):
    def __init__(self, id: int, name: str, title: str, type: str, resultType: int, visibility: str,
                 model_uuid: str, path: str):

        self.model_uuid = model_uuid
        self.path = path

        super().__init__(id, name, title, type, resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputModelSave(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
