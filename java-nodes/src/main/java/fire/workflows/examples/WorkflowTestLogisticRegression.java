package fire.workflows.examples;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.examples.NodeTestLogisticRegression;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.sql.SparkSession;

public class WorkflowTestLogisticRegression {


    public static void main(String[] args) throws Exception {

        // create spark context
        //SparkSession ctx = CreateSparkContext.createSession(args);;
        SparkSession spark = CreateSparkContext.createSession(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(spark, workflowContext);
        executewftest(jobContext);
        // stop the context
        spark.stop();
    }


    private static void executewftest(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/adult/adult.csv", DatasetType.CSV, ",",
                "age workclass fnlwgt education education_num marital_status occupation relationship race sex capital_gain capital_loss hours_per_week native_country income",
                "DOUBLE STRING DOUBLE STRING DOUBLE STRING STRING STRING STRING STRING DOUBLE DOUBLE DOUBLE STRING STRING",
                "numeric text numeric text numeric text text text text text numeric numeric numeric text text");
        wf.addNode(csv1);

        String inputCols = "income";
        String outputCols = "label";
        NodeStringIndexer nsi = new NodeStringIndexer(3, "nsi", inputCols, outputCols);
        wf.addLink(csv1, nsi);

        NodeVectorAssembler vectorAssembler = new NodeVectorAssembler(4, "vector assembler");
        String[] assmblerInputCols = { "age", "fnlwgt", "education_num" };
        vectorAssembler.inputCols = assmblerInputCols;
        vectorAssembler.outputCol = "numFeatures";
        wf.addLink(nsi, vectorAssembler);

        NodeTestLogisticRegression lr = new NodeTestLogisticRegression(5, "nlr");
        lr.featuresCol = "numFeatures";
        lr.labelCol = "label";
        lr.maxIter = 1000;
        lr.regParamGrid = "0.1 0.01";
        wf.addLink(vectorAssembler, lr);

        // pipeline
        NodePipeline pipeline = new NodePipeline(111, "pipeline");
        wf.addLink(lr, pipeline);

        // binary classification evaluator
        NodeBinaryClassificationEvaluator evaluator = new NodeBinaryClassificationEvaluator(7, "bc evaaluator", "label" ,"areaUnderROC");
        //evaluator.rawPredictionCol="rawPrediction";
        wf.addLink(pipeline, evaluator);

        // cross validator
        NodeCrossValidator crossValidator = new NodeCrossValidator(8, "cross validator");
        crossValidator.numFolds = 2;
        //wf.addLink(pipeline, crossValidator);
        wf.addLink(evaluator, crossValidator);


        NodePredict predict = new NodePredict(9, "predict");
        wf.addLink(vectorAssembler, predict);
        wf.addLink(crossValidator, predict);

        fire.nodes.util.NodePrintFirstNRows npr = new NodePrintFirstNRows(10 ,"npr", 10);
        wf.addLink(predict, npr);


        wf.execute(jobContext);

    }
}
