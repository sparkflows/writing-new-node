from enum import *

from fire.output.output import *
from fire.workflowcontext import *
from fire.workflowengine.fireschema import *


class RunningOn(Enum):
    LOCALLY = 0
    LOCALLY_SYNCHRONOUS = 1
    CLUSTER = 2
    DATABRICKS = 3
    SCHEDULER = 4


class JobContext:
    def __init__(self, cluster: bool, workflowContext: WorkflowContext):

        spark = None

        if cluster:
            spark = SparkSession.builder.appName("Create Workflow") \
                .config("spark.some.config.option", "some-value").getOrCreate()

            sparkContext: SparkContext = spark.sparkContext
            hadoopConfDir = "/etc/hadoop/conf"

            sparkContext._jsc.hadoopConfiguration().addResource(
                "file://" + hadoopConfDir + "/hdfs-site.xml")
            sparkContext._jsc.hadoopConfiguration().addResource(
                "file://" + hadoopConfDir + "/core-site.xml")
            sparkContext._jsc.hadoopConfiguration().addResource(
                "file://" + hadoopConfDir + "/hive-site.xml")

        else:
            spark = SparkSession.builder.master("local").appName("Create Workflow") \
                .config("spark.some.config.option", "some-value").getOrCreate()

        self.spark = spark

        self.outputs111: List[Output] = []

        self.runningOn = RunningOn.LOCALLY

        self.workflow: Workflow = None

        self.outputSchema: FireSchema = FireSchema()
        self.executeTillNodeId = -1

        self.workflowContext = workflowContext

        self.stopExecution = False

        self.targetCol = ''

    def saveDataFrameToJobContext(
            self,
            df: DataFrame,
            id: int,
            name: str,
            title: str):

        if df is not None:
            outputTable = OutputTable.dataFrameToOutputTable(id, title, df)
            self.outputs111.append(outputTable)

    def outSparkContext(self):

        applicationId = self.spark.sparkContext.applicationId
        outputText = OutputText(
            9999,
            "Application Id",
            "Application Id",
            applicationId,
            resultType=3,
            visibility="EXPANDED")
        self.workflowContext.outText(outputText)

        uiWebUrl = self.spark.sparkContext.uiWebUrl
        outputText = OutputText(
            9999,
            "uiWebUrl",
            "uiWebUrl",
            uiWebUrl,
            resultType=3,
            visibility="EXPANDED")
        self.workflowContext.outText(outputText)


class InputSchemaContext:
    def __init__(self, workflow):
        self.workflow = workflow
