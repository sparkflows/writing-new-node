package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.util.NodePrintFirstNRows;
import fire.nodes.ml.NodeStringIndexer;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 4/20/16.
 */
public class WorkflowStringIndexer {


    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        executewfHousingLinearRegression(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void executewfHousingLinearRegression(JobContext jobContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/housing.csv", DatasetType.CSV, ",",
                "id price lotsize bedrooms bathrms stories driveway recroom fullbase gashw airco garagepl prefarea",
                "string double double double double double string string string string string double string",
                "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining = "price";

        wf.addNode(csv1);

        // string indexer node
        NodeStringIndexer stringIndexer =
                new NodeStringIndexer(10, "NodeStringIndexer node", "bedrooms", "bedrooms_idx");

        wf.addLink(csv1, stringIndexer);

        NodePrintFirstNRows nprf= new NodePrintFirstNRows(11, "nprf", 23);
        wf.addLink(stringIndexer, nprf);

        wf.execute(jobContext);
    }

}
