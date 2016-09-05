package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDropColumns;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sony on 4/6/2016.
 */
public class WorkFlowColumnDrop {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            columDropwf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void columDropwf(JobContext jobContext) throws Exception {


        Workflow wf = new Workflow();


        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(structured);

        NodeDropColumns  ndc= new NodeDropColumns(2, "ndc", "label");

        wf.addLink(structured, ndc);

        NodePrintFirstNRows npfr = new NodePrintFirstNRows(3, "npfr", 3);
        wf.addLink(ndc, npfr);
        wf.execute(jobContext);


    }
}
