package fire.examples.workflow.save;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.save.NodeSaveParquet;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowParquet {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        parquet(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void parquet(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured parquet = new NodeDatasetStructured(1, "dataset node", "data/people.parquet", DatasetType.PARQUET);
        wf.addNode(parquet);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        wf.addLink(parquet, nodePrintFirstNRows);

        // save as parquet files
        NodeSaveParquet nodeSaveParquet = new NodeSaveParquet(3, "save parquet", "output_parquet");
        wf.addLink(nodePrintFirstNRows, nodeSaveParquet);

        // execute the workflow
        wf.execute(jobContext);

    }

}
