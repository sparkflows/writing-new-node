package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 4/22/16.
 */
public class WorkflowCorrelation {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new JsonWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        correlationwf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // correlationwf workflow
    private static void correlationwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");

        wf.addNode(csv1);


        NodeCorrelation correlation = new NodeCorrelation(6, "correlation", "c1 c2 c3 c4");
        wf.addLink(csv1, correlation);

        NodePrintFirstNRows print = new NodePrintFirstNRows(7, "print", 100);
        wf.addLink(correlation, print);

        wf.execute(jobContext);

    }

}
