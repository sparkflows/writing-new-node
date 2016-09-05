package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 4/30/16.
 */
public class WorkflowDatetime {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            datetimewf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void datetimewf(JobContext jobContext) throws Exception {


        Workflow wf = new Workflow();


        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/datetimesample.csv", DatasetType.CSV, "\t",
                "id dt prodid", "int timestamp long", "a yyyy-mm-dd a",
                "numeric numeric numeric numeric");

        structured.schema.columnFormats[1] = "yyyy-mm-dd hh:mm:ss";
        wf.addNode(structured);

        NodePrintFirstNRows npfr = new NodePrintFirstNRows(3, "npfr", 3);
        wf.addLink(structured, npfr);
        wf.execute(jobContext);


    }

}
