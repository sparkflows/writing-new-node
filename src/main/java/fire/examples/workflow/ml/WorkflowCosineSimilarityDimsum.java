package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeCosineSimilarityDimsum;
import fire.nodes.ml.NodeCountVectorizer;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by nikhilshekhar on 8/14/16.
 */
public class WorkflowCosineSimilarityDimsum {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        cosineDimsum(jobContext);

        // stop the context
        ctx.stop();
    }

    // one hot encoder workflow
    private static void cosineDimsum(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/dimsum_test.csv", DatasetType.CSV, ",",
                "c1 feature", "double string",
                "numeric text");

        wf.addNode(csv1);

        NodeCosineSimilarityDimsum dimsumSimilarity = new NodeCosineSimilarityDimsum(2," DIMSUM similarity");
        dimsumSimilarity.similarityFeatureColumn = "feature";
        dimsumSimilarity.seperator = ":";
        dimsumSimilarity.threshold = 0.1;
        wf.addLink(csv1,dimsumSimilarity);


        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(4, "print first 3 rows", 3);
        wf.addLink(dimsumSimilarity, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }
}
