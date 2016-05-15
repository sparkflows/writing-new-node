package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 4/9/16.
 */
public class WorkflowHousingPrediction {

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

        // vector assembler
        String features = "lotsize bedrooms bathrms stories";
        NodeVectorAssembler assembler = new NodeVectorAssembler(4, "feature vector assembler", features, "features");
        wf.addLink(csv1, assembler);

        // split node
        NodeSplit split = new NodeSplit(7, "split node");
        split.fraction1 = .7;
        wf.addLink(assembler, split);

        //TODO:tns add cross-validation
        NodePredict prediction = new NodePredict(9,"predict");
        wf.addLink(split, prediction);

        //Build rfc model : pass - feature vector column name and label column name
        NodeRandomForestRegression rfr = new NodeRandomForestRegression(8, "random forest", "features", "price");
        rfr.numTrees = 3;
        wf.addLink(split,rfr);
        wf.addLink(rfr, prediction);

        // evaluator
        NodeRegressionEvaluator re = new NodeRegressionEvaluator(15, "regression evaluator", "price" ,"rmse");
        wf.addLink(prediction, re);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(20, "print first 3 rows", 3);
        wf.addLink(re, nodePrintFirstNRows);

        wf.execute(jobContext);
    }
}
