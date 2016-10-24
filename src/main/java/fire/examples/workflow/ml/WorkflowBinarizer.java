package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeBinarizer;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by nikhilshekhar
 */
public class WorkflowBinarizer {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        binarizer(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // one hot encoder workflow
    private static void binarizer(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/binarized.csv", DatasetType.CSV, ",",
                "c1 c2 c3", "double string double",
                "numeric text numeric");

        wf.addNode(csv1);

        // Binarizer
        NodeBinarizer nodeBinarizer = new NodeBinarizer(2, "Binarizer node");
        nodeBinarizer.inputCol = "c3";
        nodeBinarizer.outputCol = "binarized";
        nodeBinarizer.threshold = 0.2;
        wf.addLink(csv1, nodeBinarizer);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(4, "print first 3 rows", 3);
        wf.addLink(nodeBinarizer, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);
        
    }


}

