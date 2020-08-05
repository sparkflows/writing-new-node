from fire.workflowengine import Node, InputSchemaContext, FireSchema, JobContext


class NodeTestColumnFilter(Node):

    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            parameters_mapping: dict):
        super().__init__(id, name, description)
        self.outputCols = self.getListOfValues(
            parameters_mapping['outputCols'])

    def execute(self, job_context: JobContext):
        print("Executing NodeTestColumnFilter : " + str(id))
        self.df = self.dataFrame.select(
            list(map(lambda col_name: str(col_name.strip('"')), self.outputCols)))

        print("FINISHED EXECUTING NodeTestColumnFilter")
        self.pass_dataframe_to_next_nodes_and_execute(job_context, self.df)

    def getOutputSchema(
            self,
            context: InputSchemaContext,
            inputSchema: FireSchema):
        result = inputSchema.getSchemaForColumns(self.outputCols)
        return result

