from fire.output import *

import requests
import numpy as np
import pandas as pd
import pprint

class WorkflowContext:
    def __init__(self):
        self.curNodeId = 9999

    def sendMessage(self, message: str):
        print("Sending Message")

    def outStr(self, id:int, text: str):
        print("Output String")

    def outNameValue(self, nm: str, val: str):
        print("Output Name Value")

    def outHeader(self, header: OutputHeader):
        print("Output Header")

    def outHeaderValues(self, id: int, name: str, tstr: str):
        print("Output Header Values")

    def outSuccess(self, success: OutputSuccess):
        print("Output Success")

    def outFailure(self, failure: OutputFailure):
        print("Output Failure")

    def outFailureValues(self, id: int, name: str, tstr: str):
        print("Output Failure")

    def outTable(self, table: OutputTable):
        print("Output Table")

    def outModel(self, model: OutputModel):
        print("Output Model")

    def outSchema(self, id: int, title: str, df: pd.DataFrame):
        print("Output Schema")

    def outDataFrame(self, id: int, title: str, df: pd.DataFrame):
        print("Output DataFrame")

    def outPandasDataframe(self, id: int, title: str, df: pd.DataFrame):
        print("Output Pandas Dataframe")

    def outNumpy1darray(self, id: int, title: str, df: np.ndarray):
        print("Output Numpy ndarray")

    def outNumpy2darray(self, id: int, title: str, df: np.ndarray):
        print("Output Numpy ndarray")

class RestWorkflowContext(WorkflowContext):
    def __init__(self, webserverURL: str, jobId: str):

        self.webserverURL = webserverURL
        self.jobId = jobId

    def sendMessage(self, message: str):
        print("Sending Message" + message)

        if not self.webserverURL.lower().startswith("http"):
            print("Not sending message to fire server as the post back URL is not http")
            return ""

        data = { "jobId" : self.jobId,
                       "message" : message}

        # send POST to webserverURL with parameters
        try:
            result = requests.post(url=self.webserverURL, data=data)
        except Exception as e:
            errstr = "Error : " + str(e)
            print(errstr)
            result = errstr

        return result

    def outStr(self, id:int, text: str):
        text = text.replace('\n', '<br>')

        outputText = OutputText(id, "name", "title", text, resultType=3,
                                visibility="EXPANDED")
        self.outText(outputText)

    def outNameValue(self, nm: str, val: str):
        outputText = OutputText(self.curNodeId, nm, "title", val, resultType=3,
                                visibility="EXPANDED")
        self.outText(outputText)

    def outText(self, text: OutputText):
        str = text.toJSON1()
        self.sendMessage(str)

    def outHeader(self, header: OutputHeader):
        tstr = header.toJSON1()
        self.sendMessage(tstr)

    def outHeaderValues(self, id: int, name: str, tstr: str):
        outputHeader: OutputHeader = OutputHeader(id, name, name, tstr, resultType=3, visibility="EXPANDED")
        self.outHeader(outputHeader)

    def outSuccess(self, success: OutputSuccess):
        str = success.toJSON1()
        self.sendMessage(str)

    def outFailure(self, failure: OutputFailure):
        str = failure.toJSON1()
        self.sendMessage(str)

    def outFailureValues(self, id: int, name: str, tstr: str):
        outputFailure: OutputFailure = OutputFailure(id, name, name, tstr, resultType=3, visibility="EXPANDED")
        self.outFailure(outputFailure)

    def outTable(self, table: OutputTable):
        str = table.toJSON1()
        self.sendMessage(str)

    def outModel(self, model: OutputModel):
        str = model.toJSON1()
        self.sendMessage(str)

    def outSchema(self, id: int, title: str, df: pd.DataFrame):

        if df == None:
            return

        d = [[]]

        r1 = [f.name for f in df.schema.fields]  #column name
        column_types = [f.dataType for f in df.schema.fields] #column type
        r2 = list(map(lambda col_type: str(col_type), column_types))

        d[0] = r1
        d.append(r2)

        outputTable = OutputTable(id=id, name="Schema", title=title, cellValues=d, resultType=3, visibility="COLLAPSED")

        self.outTable(outputTable)


    def outDataFrame(self, id: int, title: str, df: pd.DataFrame):

        if df is None:
            return

        outputTable = OutputTable.dataFrameToOutputTable(id, title, df)

        self.outTable(outputTable)

    def outPandasDataframe(self, id: int, title: str, df: pd.DataFrame):
        outputTable = OutputTable.pandasDataFrametoOutputTable(id, title, df)

        self.outTable(outputTable)

    def outNumpy1darray(self, id: int, title: str, arr: np.ndarray):
        sss = arr.tostring()
        sss = ""
        c = arr.shape[0]
        for k in range(c):
            sss += str(arr[k]) + " "
        self.outNameValue(title, sss)

    def outNumpy2darray(self, id: int, title: str, arr: np.ndarray):
        outputTable = OutputTable.convertNumpyndarraytoOutputTable(id, title, arr)

        self.outTable(outputTable)

class SynchronousWorkflowContext(RestWorkflowContext):
    def __init__(self, webserverURL: str, jobId: str):
        self.resultsJson: List[str] = []
        super().__init__(webserverURL, jobId)

    def sendMessage(self, message: str):
        self.resultsJson.append(message)

class ConsoleWorkflowContext(WorkflowContext):
    def __init__(self):
        print("Created ConsoleWorkflowContext")

    def sendMessage(self, message: str):
        print("Sending Message" + message)

    def outText(self, text: OutputText):
        str = text.toJSON1()
        print("Text" + str)

    def outSuccess(self, success: OutputSuccess):
        str = success.toJSON1()
        print("Text" + str)

    def outDataFrame(self, id: int, title: str, df: pd.DataFrame):

        if df is None:
            return

        outputTable = OutputTable.dataFrameToOutputTable(id, title, df)

        str = outputTable.toJSON1()
        pp = pprint.PrettyPrinter(indent=4)

        pp.pprint(outputTable.toJSON1())
        #print("DataFrame : " + str)