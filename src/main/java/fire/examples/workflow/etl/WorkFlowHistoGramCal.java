package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeHistoGramCal;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/5/2016.
 */
public class WorkFlowHistoGramCal  {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        histoGramwf(jobContext);

        // stop the context
        ctx.stop();
    }

    private static void histoGramwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured house_price = new NodeDatasetStructured(1, "house_price data", "data/transactions.csv", DatasetType.CSV, ",",
                "city beds baths sq_ft price", "string integer integer integer integer",
                "text numeric numeric numeric numeric");
        wf.addNode(house_price);

        NodeHistoGramCal histoGram = new NodeHistoGramCal(2, "hist");
        histoGram.inputCols = "beds";
        histoGram.bins = 3;

        wf.addLink(house_price, histoGram);
        wf.execute(jobContext);


    }
}
