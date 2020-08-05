class StringString():
    def __init__(self, str1: str, str2: str):

        self.str1 = str1
        self.str2 = str2

    def dump(self):
        d = {k: v for k, v in vars(self).items()}
        return d

    @staticmethod
    def load(d: dict):
        return StringString(**d)

    def toJSON1(self):
        tempStr = str(self.dump())

        # replace ' with \"
        tempStr = tempStr.replace("\'", "\"")

        return tempStr
