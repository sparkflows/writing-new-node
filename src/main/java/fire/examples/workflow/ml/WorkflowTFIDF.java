package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 4/14/16.
 */
public class WorkflowTFIDF {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        tfidf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // TF IDF
    private static void tfidf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/spam.csv", DatasetType.CSV, ",",
                "label sentence id", "double string double",
                "numeric text numeric");

        wf.addNode(csv1);

        // tokenizer
        NodeTokenizer tokenizer = new NodeTokenizer(2, "tokenizer node");
        tokenizer.inputCol = "sentence";
        tokenizer.outputCol = "words";
        wf.addLink(csv1, tokenizer);

        // hashing tf
        NodeHashingTF hashingTF = new NodeHashingTF(3, "hashing tf node");
        hashingTF.inputCol = "words";
        hashingTF.outputCol = "rawFeatures";
        wf.addLink(tokenizer, hashingTF);

        // idf
        NodeIDF nodeIDF = new NodeIDF(5, "idf node");
        nodeIDF.inputCol = "rawFeatures";
        nodeIDF.outputCol = "features";
        wf.addLink(hashingTF, nodeIDF);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(6, "print first 3 rows", 3);
        wf.addLink(nodeIDF, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
