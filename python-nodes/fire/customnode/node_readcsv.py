

from fire.nodes.dataset import NodeDataset
from fire.workflowengine.jobcontext import JobContext, InputSchemaContext
from fire.workflowengine.fireschema import FireSchema


class NodeReadCSV(NodeDataset):

    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            parameters_mapping: dict):
        self.path = parameters_mapping['path']
        self.separator = parameters_mapping['separator']
        self.header = parameters_mapping['header']
        self.dropMalformed = parameters_mapping['dropMalformed']

        # '["id","price","lotsize","bedrooms","bathrms","stories","driveway","recroom","fullbase","gashw","airco","garagepl","prefarea"]'

        self.outputColNames = self.getListOfValues(
            parameters_mapping['outputColNames'])
        self.outputColTypes = self.getListOfValues(
            parameters_mapping['outputColTypes'])
        self.outputColFormats = self.getListOfValues(
            parameters_mapping['outputColFormats'])

        super().__init__(id, name, description, self.path)

    def execute(self, job: JobContext):
        print("Executing ReadCSV node : " + str(self.id))

        s = FireSchema()
        s.colNames = self.outputColNames
        s.colTypes = self.outputColTypes
        s.colFormats = self.outputColFormats

        reader = job.spark.read.format("csv")\
            .option("header", self.header)\
            .option("sep", self.separator)

        if self.dropMalformed == 'true':
            reader.option("mode", "DROPMALFORMED")

        if not self.gettingOutputSchema:
            ss = s.toSparkSQLStructType()
            reader.option("inferSchema", "false")
            reader.schema(ss)
            print("getting output schema")
        else:
            print("not getting output schema")
            reader.option("inferSchema", "true")

        self.df = reader.load(self.path)

        self.pass_dataframe_to_next_nodes_and_execute(job, self.df)

    def getOutputSchema(
            self,
            context: InputSchemaContext,
            inputSchema: FireSchema):
        fireSchema = FireSchema()
        fireSchema.colNames = self.outputColNames
        fireSchema.colTypes = self.outputColTypes
        fireSchema.colFormats = self.outputColFormats
        fireSchema.mlTypes = self.outputColFormats

        return fireSchema

