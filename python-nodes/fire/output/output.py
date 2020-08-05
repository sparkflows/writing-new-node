from datetime import *


class Output:
    def __init__(self, id: int, name: str, title: str, type: str, resultType: int, visibility: str):
        self.id = id
        self.name = name
        self.title = title
        self.type = type

        self.resultType = resultType
        self.visibility = visibility
        #self.time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.title = datetime.now().strftime('%b %d, %Y %H:%M:%S %p')




