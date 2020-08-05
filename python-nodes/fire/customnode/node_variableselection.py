from fire.workflowengine import Node, InputSchemaContext, FireSchema, JobContext
import scorecardpy as sc

class NodeVariableSelection(Node):

    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            parameters_mapping: dict):
        super().__init__(id, name, description)
        self.targetCol = parameters_mapping['targetCol']

    def execute(self, job_context: JobContext):
        print("Executing NodeVariableSelection : " + str(id))

        # convert to Pandas dataframe
        pandas_df = self.dataFrame.toPandas()

        # variable selection with scorecard
        df_var_filter = sc.var_filter(pandas_df, y=self.targetCol)

        print(df_var_filter)

        # convert result to Spark dataframe
        spark_df = job_context.spark.createDataFrame(df_var_filter)
        print("FINISHED EXECUTING NodeVariableSelection")

        # pass dataframe to the next nodes
        self.pass_dataframe_to_next_nodes_and_execute(job_context, spark_df)
