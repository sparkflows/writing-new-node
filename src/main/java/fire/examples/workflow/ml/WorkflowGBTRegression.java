package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeDateTimeFieldExtract;
import fire.nodes.ml.*;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/10/2016.
 */
public class WorkflowGBTRegression {

    public static void main(String[] args) throws Exception {
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfForecastCityBikeShare(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfForecastCityBikeShare(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured clicks = new NodeDatasetStructured(1, "csv1 node", "data/Bike_Sharing_Demand/train.csv", DatasetType.CSV, "," ,
                "datetime season holiday workingday weather temp atemp humidity windspeed casual registeredcount count",
                "timestamp int int int int double double int double int int int","yyyy-MM-dd HH:mm:SS",
                "text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");
        wf.addNode(clicks);
        //2011-01-01 13:00:00,1,0,0,2,18.86,22.725,72,19.9995,47,47,94

        String colNames = "count";
        String outputTypes = "double";
        NodeCastColumnType ncc = new NodeCastColumnType(11, "ncc", colNames, outputTypes);
        wf.addLink(clicks, ncc);

        NodeDateTimeFieldExtract dtfe = new NodeDateTimeFieldExtract(2, "dtfe");
        dtfe.inputCol = "datetime";
        //dtfe.datetime_format ="yyyy-MM-dd HH:mm:SS";
        dtfe.values = "year month dayofmonth hour";
        wf.addLink(ncc, dtfe);

        NodeVectorAssembler nva = new NodeVectorAssembler(3, "nva");
        String [] input = {"season", "holiday","workingday","weather","temp","atemp","humidity","windspeed","datetime_year","datetime_month","datetime_dayofmonth","datetime_hour"};
        nva.inputCols =  input;
        nva.outputCol = "feature_vector";
        wf.addLink(dtfe,nva);

        NodeVectorIndexer nvi = new NodeVectorIndexer(4, "nvi");
        nvi.inputCol = "feature_vector";
        nvi.outputCol = "feature_vector_index";
        nvi.maxCategories = 31;
        wf.addLink(nva, nvi);

        NodeSplit split = new NodeSplit(5, "split");
        split.splitRatio = "0.8 0.2";
        wf.addLink(nvi, split);

        NodeGBTRegression ngbr = new NodeGBTRegression(6, "ngbr");
        ngbr.featuresCol = "feature_vector_index";
        ngbr.labelCol = "count";
        ngbr.maxIter= 4;
        ngbr.maxDepth = 3;
        ngbr.maxBins = 32;
        ngbr.impurity = "variance";
        ngbr.lossType = "squared";
        ngbr.maxMemoryInMB=256;
        ngbr.minInfoGain=0.0;
        ngbr.subsamplingRate = 1.0;
        ngbr.checkpointInterval = 10;
        ngbr.minInstancesPerNode = 1;
        ngbr.stepSize = 0.1;
        wf.addLink(split, ngbr);

        NodePredict np = new NodePredict(7, "np");
        wf.addLink(ngbr, np);
        wf.addLink(split, np);

        NodeRegressionEvaluator nre = new NodeRegressionEvaluator(8, "nre");
        nre.metricName = "mse";
        nre.labelCol = "count";
        nre.predictionCol = "prediction";
        wf.addLink(np, nre);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(10, "npr", 10);
        wf.addLink(dtfe, npr);

        wf.execute(jobContext);

    }
}
