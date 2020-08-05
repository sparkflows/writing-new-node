from fire.workflowengine import Node, JobContext, RunningOn


class NodePrintNRows(Node):
    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            parameters_mapping: dict):
        self.num_rows = parameters_mapping.get('n', 10)
        self.title = parameters_mapping.get('title')

        super().__init__(id, name, description)

    def execute(self, job_context: JobContext):
        print("Executing PrintNRows node : " + str(id))
        # self.dataFrame.show(5)

        if job_context.runningOn == RunningOn.LOCALLY_SYNCHRONOUS and job_context.executeTillNodeId > -1:
            print("")
        else:
            job_context.workflowContext.outDataFrame(
                self.id, self.title, self.dataFrame)

        self.pass_dataframe_to_next_nodes_and_execute(job_context, self.dataFrame)

