from fire.workflowengine.workflow import *
from fire.output.output import Output

import pandas as pd
import numpy as np


class OutputTable(Output):
    def __init__(self, id: int, name: str, title: str, cellValues: [[]], resultType: int, visibility: str):

        self.cellValues = cellValues

        super().__init__(id, name, title, "table", resultType, visibility)

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

    @staticmethod
    def dataFrameToOutputTable(id: int, title: str, df: pd.DataFrame):

        output_rows = df.take(10)

        d = [[]]

        r1 = [f.name for f in df.schema.fields]  #column name
        column_types = [f.dataType for f in df.schema.fields] #column type
        r2 = list(map(lambda col_type: str(col_type), column_types))

        d[0] = r1
        d.append(r2)

        for row in output_rows:
            data_row = []
            for element in row:
                data_row.append(str(element))
            d.append(data_row)

        outputTable = OutputTable(id=id, name="", title=title, cellValues=d, resultType=3, visibility="EXPANDED")

        return outputTable

    # Converts the given Pandas DataFrame to OutputTable
    @staticmethod
    def pandasDataFrametoOutputTable(id: int, title: str, df : pd.DataFrame):

        d = [[]]
        col_names = list(df.columns)
        col_types = []
        for i in range(len(col_names)):
            col_name = col_names[i]
            col_types.append(df[col_name].dtype.name)
        d[0] = col_names
        d.append(col_types)

        num_rows = 100
        df_lists = df.head(num_rows).values.tolist()

        for k in range(len(df_lists)):
            d.append(df_lists[k])

        outputTable = OutputTable(id=id, name="Pandas DataFrame",
                                  title=title, cellValues=d, resultType=3, visibility="EXPANDED")
        return outputTable

    # Converts the given Numpy ndarray to OutputTable
    @staticmethod
    def convertNumpyndarraytoOutputTable(id: int, title: str, df: np.ndarray):

        d = [[]]
        num_rows = 100
        r, c = df.shape

        if num_rows > r:
            num_rows = r

        cur_row = []
        for k in range(c):
            cur_row.append(k)

        d[0] = cur_row

        for k in range(num_rows):
            cur_row = []
            temparr = df[k]

            for z in range(len(temparr)):
                cur_row.append(temparr[z])

            d.append(cur_row)

        #d[0] = df[0]

        #for k in range(num_rows - 1):
        #    d.append(df[k + 1])

        outputTable = OutputTable(id=id, name="Numpy ndarray ",
                                  title=title, cellValues=d, resultType=3, visibility="EXPANDED")
        return outputTable


if __name__ == '__main__':
    print("Testing OutputTable")

    d = {'col1': [1, 2], 'col2': [3, 4]}
    df = pd.DataFrame(data=d)

    outputTable = OutputTable.pandasDataFrametoOutputTable(1, "title", df)
    print(outputTable.toJSON())

    arr = np.ndarray(shape=(2, 2), dtype=float, order='F')
    print(arr)

    outputTable = OutputTable.convertNumpyndarraytoOutputTable(1, "title", arr)
    print(outputTable.toJSON())

    print("DONE")
