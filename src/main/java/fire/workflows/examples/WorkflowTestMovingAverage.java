package fire.workflows.examples;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.examples.NodeTestMovingAverage;
import fire.nodes.examples.NodeTestPrintFirstNRows;
import fire.nodes.examples.NodeTestZipWithIndex;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.sql.SparkSession;

public class WorkflowTestMovingAverage {

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
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(structured);

        // moving average node
        NodeTestMovingAverage ntm = new NodeTestMovingAverage(2, "ntm");
        ntm.setInputCol("f2");
        wf.addLink(structured, ntm);

        // print first 10 rows
        NodeTestPrintFirstNRows printFirstNRows = new NodeTestPrintFirstNRows(3, "print first rows", 10);
        wf.addLink(ntm, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
