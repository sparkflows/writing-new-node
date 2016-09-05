package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 4/14/16.
 */
public class WorkflowTrainValidationSplit {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        trainvalidationsplitwf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // http://spark.apache.org/docs/latest/ml-guide.html#example-model-selection-via-cross-validation
    private static void trainvalidationsplitwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/spam1.csv", DatasetType.CSV, ",",
                "id text label", "double string double",
                "numeric text numeric");
        wf.addNode(csv1);

        // tokenizer
        NodeTokenizer tokenizer = new NodeTokenizer(2, "tokenizer node");
        tokenizer.inputCol = "text";
        tokenizer.outputCol = "words";
        wf.addLink(csv1, tokenizer);

        // hashing tf
        NodeHashingTF hashingTF = new NodeHashingTF(3, "hashing tf node");
        hashingTF.inputCol = "words";
        hashingTF.outputCol = "features";
        wf.addLink(tokenizer, hashingTF);

        // logistic regression
        NodeLogisticRegression logisticRegression = new NodeLogisticRegression(5, "logistic regression node");
        logisticRegression.maxIter = 1000;
        logisticRegression.regParam = .01;
        logisticRegression.featuresCol = "features";
        logisticRegression.labelCol = "label";
        logisticRegression.regParamGrid = "0.1 0.01";
        wf.addLink(hashingTF, logisticRegression);

        // pipeline
        NodePipeline pipeline = new NodePipeline(10, "pipeline");
        wf.addLink(logisticRegression, pipeline);

        // binary classification evaluator
        NodeBinaryClassificationEvaluator evaluator = new NodeBinaryClassificationEvaluator(8, "bc evaaluator", "label" ,"areaUnderROC");
        wf.addLink(pipeline, evaluator);

        // train validation split
        NodeTrainValidationSplit trainValidationSplit = new NodeTrainValidationSplit(18, "train validation split");
        trainValidationSplit.trainRatio = .8;
        //wf.addLink(pipeline, trainValidationSplit);
        wf.addLink(evaluator, trainValidationSplit);

        // csv1 node
        NodeDatasetStructured test = new NodeDatasetStructured(22, "csv1 node", "data/spam1.csv", DatasetType.CSV, ",",
                "id text label", "double string double",
                "numeric text numeric");

        // predict
        NodePredict predict = new NodePredict(25, "predict");
        wf.addLink(test, predict);
        wf.addLink(trainValidationSplit, predict);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(6, "print first 3 rows", 3);

        // execute the workflow
        wf.execute(jobContext);

    }

}
