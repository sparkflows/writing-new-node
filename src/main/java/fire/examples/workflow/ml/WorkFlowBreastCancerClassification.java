package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeRowFilter;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 3/26/2016.
 */
public class WorkFlowBreastCancerClassification {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfBreastCancerClassification(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfBreastCancerClassification(JobContext jobContext) throws Exception {
        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/breast_cancer/breast-cancer-wisconsin.data", DatasetType.CSV, ",",
                "sampleCodeNumber clumpThickness uniformityOfCellSize uniformityOfCellShape marginalAdhesion singleEpithelialCellSize bareNuclei blandChromatin normalNucleoli mitoses class",
                "double double double double double double string double double double double",
                "numeric numeric numeric numeric numeric numeric text numeric numeric numeric numeric");
        wf.addNode(csv1);

        NodeRowFilter rf = new NodeRowFilter(2, "filter out row's with ?" ,"bareNuclei != '?'");
        wf.addLink(csv1, rf);

        NodeCastColumnType cct = new NodeCastColumnType(3, "cct", "SampleCodeNumber bareNuclei", "int double");
        wf.addLink(rf, cct);

        NodeStringIndexer labelIndexer = new NodeStringIndexer(4,"label indexer", "class" ,"label");
        wf.addLink(cct, labelIndexer);

        //select features
        String features = "clumpThickness uniformityOfCellSize uniformityOfCellShape marginalAdhesion singleEpithelialCellSize bareNuclei blandChromatin normalNucleoli mitoses";

        NodeVectorAssembler assembler = new NodeVectorAssembler(5, "feature vector assembler", features,"features_vector");
        wf.addLink(labelIndexer,assembler);

        Node split = new NodeSplit(6, "split node","0.8 0.2");
        wf.addLink(assembler, split);


        NodeRandomForestClassifier rfc = new NodeRandomForestClassifier(7,"random forest","features_vector","label");
        rfc.numTrees = 20;
        rfc.maxDepth = 3;
        rfc.seed = 5043L;
        wf.addLink(split,rfc);

        NodePredict prediction = new NodePredict(8,"predict");
        wf.addLink(rfc, prediction);
        wf.addLink(split, prediction);

        NodeMulticlassClassificationEvaluator mccev = new NodeMulticlassClassificationEvaluator(9, "mcc evaaluator", "label" ,"precision");
        wf.addLink(prediction, mccev);

        NodeColumnFilter ncf = new NodeColumnFilter(10, "column filter", "sampleCodeNumber class label prediction");
        wf.addLink(mccev, ncf);

        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(11, "print rows", 146);
        wf.addLink(ncf, nodePrintFirstNRows);

        wf.execute(jobContext);

    }
}
