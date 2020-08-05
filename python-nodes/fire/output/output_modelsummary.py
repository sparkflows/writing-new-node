from fire.output.output import Output


class OutputModelSummary(Output):
    def __init__(self, id: int, name: str, title: str, type: str, resultType: int, visibility: str,
                 contentType: str, contentString: str, contentArray:str):

        self.contentType = contentType
        self.contentArray = contentArray
        self.contentString = contentString

        super().__init__(id, name, title, type, resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputModelSummary(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
