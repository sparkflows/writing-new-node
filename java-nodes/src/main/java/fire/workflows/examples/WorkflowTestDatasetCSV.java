package fire.workflows.examples;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.examples.NodeTestConcatColumns;
import fire.nodes.examples.NodeTestDatasetCSV;
import fire.nodes.examples.NodeTestPrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.sql.SparkSession;

public class WorkflowTestDatasetCSV {

    public static void main(String[] args) {

        // create spark session
        SparkSession ctx = CreateSparkContext.createSession(args);;
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            wf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        // stop the context
        ctx.stop();
    }

    // create and execute the workflow
    private static void wf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // NODE IDS HAVE TO BE UNIQUE
        // structured node
        NodeTestDatasetCSV datasetCSV = new NodeTestDatasetCSV(1, "datasetCSV",
                     "data/cars.csv",",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");

        // column filter node
        NodeColumnFilter filter = new NodeColumnFilter(5, "filter node", "f1 f2");
        wf.addLink(datasetCSV, filter);

        // print first 10 rows
        NodeTestPrintFirstNRows printFirstNRows = new NodeTestPrintFirstNRows(15, "print first rows", 10);
        wf.addLink(filter, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }
}
