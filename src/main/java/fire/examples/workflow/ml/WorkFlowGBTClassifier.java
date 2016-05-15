package fire.examples.workflow.ml;


import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeConcatColumns;
import fire.nodes.etl.NodeDropColumns;
import fire.nodes.ml.*;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/9/2016.
 */
public class WorkFlowGBTClassifier {


    public static void main(String[] args){
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfCTRPrediction(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfCTRPrediction(JobContext jobContext) {

        Workflow wf = new Workflow();

        NodeDatasetStructured clicks = new NodeDatasetStructured(1, "csv1 node", "data/ctr_avazu_dataset/train_sample.csv", DatasetType.CSV, "," ,
                "id click hour C1 banner_pos site_id site_domain site_category app_id app_domain app_category device_id device_ip device_model device_type device_conn_type C14 C15 C16 C17 C18 C19 C20 C21",
                "string double string string string string string string string string string string string string string string string string string string string string string string",
                "text numeric text text text text text text text text text text text text text text text text text text text text text text");

        wf.addNode(clicks);

        String colNames = "hour C1 banner_pos device_type device_conn_type C14 C15 C16 C17 C18 C19 C20 C21";
        //ncc.inputType = "string string string string string string string string string string string string string";
        String outputTypes = "int int int int int int int int int int int int int";
        NodeCastColumnType ncc = new NodeCastColumnType(2, "ncc", colNames, outputTypes);
        wf.addLink(clicks, ncc);


        NodeConcatColumns nconcat = new NodeConcatColumns(3, "nconcat", "site_id app_id");
        nconcat.outputCol = "site_app_id";
        wf.addLink(ncc, nconcat);

        String columnNames = "site_id site_domain site_category app_id app_domain app_category device_id device_ip device_model";
        NodeDropColumns ndc = new NodeDropColumns(4, "ndc", columnNames);
        wf.addLink(nconcat, ndc);

        NodeStringIndexer nsi = new NodeStringIndexer(5, "nsi", "site_app_id", "site_app_id_index");
        nsi.handleInvalid = "error";
        wf.addLink(ndc, nsi);

        NodeOneHotEncoder nohe = new NodeOneHotEncoder(6,"nohe", "site_app_id_index", "site_app_id_nohe");
        wf.addLink(nsi, nohe);

        columnNames = "site_app_id site_app_id_index";
        NodeDropColumns ndc1 = new NodeDropColumns(41, "ndc1", columnNames);
        wf.addLink(nohe, ndc1);

        NodeStringIndexer labelIndexer = new NodeStringIndexer(7,"label indexer", "click", "label");
        wf.addLink(ndc1, labelIndexer);

        //String inputCols= "hour C1 banner_pos device_type device_conn_type C14 C15 C16 C17 C18 C19 C20 C21 site_app_id_nohe";
        String inputCols="device_type device_conn_type";
        String outputCol = "features";
        NodeVectorAssembler nva = new NodeVectorAssembler(8, "nva", inputCols, outputCol);
        wf.addLink(labelIndexer, nva);


        NodeVectorIndexer nvi = new NodeVectorIndexer(9, "nvi");
        nvi.inputCol = "features";
        nvi.outputCol = "features_index_vec";
        nvi.maxCategories = 8;
        wf.addLink(nva, nvi);

        NodeSplit split = new NodeSplit(10, "split");
        split.splitRatio = "0.5 0.5";
        wf.addLink(nvi, split);

        NodeGBTClassifier gbtc = new NodeGBTClassifier(11,"gbm");
        gbtc.featuresCol = "features_index_vec";
        gbtc.labelCol = "label";
        gbtc.maxDepth = 3;
        gbtc.maxBins = 32;
        gbtc.impurity="gini";
        gbtc.maxMemoryInMB= 256;
        gbtc.minInstancesPerNode=1;
        gbtc.maxIter=3;
        gbtc.minInfoGain= 0.0;
        gbtc.subsamplingRate= 1.0;
        gbtc.checkpointInterval=10;
        gbtc.cacheNodeIds=false;
        gbtc.stepSize=0.1;
        gbtc.lossType="logistic";
        wf.addLink(split, gbtc);


        NodePredict prediction = new NodePredict(12,"predict");
        wf.addLink(gbtc, prediction);
        wf.addLink(split, prediction);

        //Good to display all metrics by default???
        NodeMulticlassClassificationEvaluator mccev = new NodeMulticlassClassificationEvaluator(13, "mcc evaaluator");
        mccev.labelCol = "label";
        mccev.metricName = "precision";
        mccev.predictionCol="prediction";
        wf.addLink(prediction, mccev);

        wf.execute(jobContext);
    }


}
