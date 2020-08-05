from fire.output.output import Output


class OutputModelEvaluation(Output):
    def __init__(self, id: int, name: str, title: str, type: str, resultType: int, visibility: str,
                 model_uuid: str, test_metrics: str, metrics: str, metrics_type: str):

        self.model_uuid = model_uuid
        self.test_metrics = test_metrics
        self.metrics = metrics
        self.metrics_type = metrics_type

        super().__init__(id=id, name=name, title=title, type=type, resultType= resultType, visibility=visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputModelEvaluation(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
