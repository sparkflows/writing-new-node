from fire.output.output import Output


class OutputModel(Output):
    def __init__(self, id: int, name: str, title: str, type: str, resultType: int, visibility: str,
                 model_uuid: str, category: str, algorithm: str, model_path: str, features: str,
                 features_importance: str, train_metrics: str, model_summary: str, ml_technology: str):

        self.model_uuid = model_uuid
        self.category = category
        self.algorithm = algorithm
        self.model_path = model_path
        self.features = features
        self.features_importance = features_importance
        self.train_metrics = train_metrics
        self.model_summary = model_summary
        self.ml_technology = ml_technology

        super().__init__(id, name, title, type, resultType, visibility)

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return OutputModel(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
