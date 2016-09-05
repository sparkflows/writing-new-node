package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeCountVectorizer;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by nikhilshekhar
 */
public class WorkflowCountVectorizer {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        countVectorizer(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // one hot encoder workflow
    private static void countVectorizer(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/spam.csv", DatasetType.CSV, ",",
                "c1 c2 c3", "double string double",
                "numeric text numeric");

        wf.addNode(csv1);

        // one hot encoder node
        NodeCountVectorizer countVectorizer = new NodeCountVectorizer(2, "onehotencoder node");
        countVectorizer.inputCol = "c2";
        countVectorizer.outputCol = "encodedcol";
        countVectorizer.vocabularySize = 3;
        countVectorizer.minDf =2;
        wf.addLink(csv1, countVectorizer);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(4, "print first 3 rows", 3);
        wf.addLink(countVectorizer, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }


}
