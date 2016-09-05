package fire.examples.workflow.save;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.save.NodeSaveJSON;
import fire.nodes.save.NodeSaveParquet;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 5/17/16.
 */
public class WorkflowJSON {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        json(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void json(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured parquet = new NodeDatasetStructured(1, "parquet node", "data/people.parquet", DatasetType.PARQUET);
        wf.addNode(parquet);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        wf.addLink(parquet, nodePrintFirstNRows);

        // save as json files
        NodeSaveJSON nodeSaveJSON = new NodeSaveJSON(3, "save json", "output_json");
        wf.addLink(nodePrintFirstNRows, nodeSaveJSON);

        // execute the workflow
        wf.execute(jobContext);

    }

}
