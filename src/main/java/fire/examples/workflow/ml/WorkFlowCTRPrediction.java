package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeReplaceMissingValue;
import fire.nodes.ml.NodeSplit;
import fire.nodes.etl.NodeDropRowsWithNull;

import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 3/26/2016.
 */

public class WorkFlowCTRPrediction {

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


   private static void executewfCTRPrediction(JobContext jobContext){

       Workflow wf = new Workflow();

       NodeDatasetStructured clicks = new NodeDatasetStructured(1, "csv1 node", "data/click_through_rate/dac_sample.txt", DatasetType.CSV, "\t",
               "label IntFeature1 IntFeature2 IntFeature3 IntFeature4 IntFeature5 IntFeature6 IntFeature7 IntFeature8 IntFeature9 IntFeature10 IntFeature11 IntFeature12 IntFeature13 CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26",
               "double double double double double double double double double double double double double double string string string string string string string string string string string string string string string string string string string string string string string string string string",
               "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text text text text text text text text text text text text text text text text text text text text text text text text text text");

       wf.addNode(clicks);


       /*NodeReplaceMissingValue rmv = new NodeReplaceMissingValue(11, "rmv");
       rmv.colNames = "CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26";
       rmv.constant = "None";
       wf.addLink(clicks, rmv);*/

       NodeReplaceMissingValue rmv = new NodeReplaceMissingValue(11, "rmv");
       rmv.colNames = "IntFeature1 IntFeature2 IntFeature3 IntFeature4 IntFeature5 IntFeature6 IntFeature7 IntFeature8 IntFeature9 IntFeature10 IntFeature11 IntFeature12 IntFeature13 CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26";
       rmv.mode = true;
       wf.addLink(clicks, rmv);

       NodeDropRowsWithNull ndr = new NodeDropRowsWithNull(2, "ndr");
       wf.addLink(rmv, ndr);

       String inputCols = "CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26";
       String outputCols = "indexedCat1 indexedCat2 indexedCat3 indexedCat4 indexedCat5 indexedCat6 indexedCat7 indexedCat8 indexedCat9 indexedCat10 indexedCat11 indexedCat12 indexedCat13 indexedCat14 indexedCat15 indexedCat16 indexedCat17 indexedCat18 indexedCat19 indexedCat20 indexedCat21 indexedCat22 indexedCat23 indexedCat24 indexedCat25 indexedCat26";
       NodeStringIndexer nsi = new NodeStringIndexer(3, "nsi", inputCols, outputCols);
       wf.addLink(ndr, nsi);

       inputCols = "indexedCat1 indexedCat2 indexedCat3 indexedCat4 indexedCat5 indexedCat6 indexedCat7 indexedCat8 indexedCat10 indexedCat11 indexedCat12 indexedCat13 indexedCat14 indexedCat15 indexedCat16 indexedCat17 indexedCat18 indexedCat19 indexedCat20 indexedCat21 indexedCat22 indexedCat23 indexedCat24 indexedCat25 indexedCat26";
       outputCols ="CatVector1 CatVector2 CatVector3 CatVector4 CatVector5 CatVector6 CatVector7 CatVector8 CatVector10 CatVector11 CatVector12 CatVector13 CatVector14 CatVector15 CatVector16 CatVector17 CatVector18 CatVector19 CatVector20 CatVector21 CatVector22 CatVector23 CatVector24 CatVector25 CatVector26";
       NodeOneHotEncoder nohe = new NodeOneHotEncoder(4, "nohe", inputCols, outputCols);
       wf.addLink(nsi, nohe);

       inputCols = "IntFeature1 IntFeature2 IntFeature3 IntFeature4 IntFeature5 IntFeature6 IntFeature7 IntFeature8 IntFeature9 IntFeature10 IntFeature11 IntFeature12 IntFeature13 CatVector1 CatVector2 CatVector3 CatVector4 CatVector5 CatVector6 CatVector7 CatVector8 indexedCat9 CatVector10 CatVector11 CatVector12 CatVector13 CatVector14 CatVector15 CatVector16 CatVector17 CatVector18 CatVector19 CatVector20 CatVector21 CatVector22 CatVector23 CatVector24 CatVector25 CatVector26";
       String outputCol="features";
       NodeVectorAssembler nvs = new NodeVectorAssembler(5, "nvas", inputCols, outputCol);
       wf.addLink(nohe, nvs);

       NodeColumnFilter ncf = new NodeColumnFilter(6, "ncf", "label features");
       wf.addLink(nvs, ncf);
       
       NodeSplit split = new NodeSplit(7, "split node");
       split.splitRatio = "0.7 0.3";
       wf.addLink(ncf, split);


       NodeLogisticRegression nlr = new NodeLogisticRegression(8, "nlr");
       nlr.featuresCol = "features";
       nlr.labelCol = "label";
       nlr.regParam = 0.0;
       nlr.maxIter = 3;
       nlr.fitIntercept = true;
       nlr.threshold= 0.5;
       nlr.standardization = true;
       nlr.elasticNetParam = 0.0;
       nlr.tol = 1e-6;

       wf.addLink(split,nlr);

       NodePredict prediction = new NodePredict(9,"predict");
       wf.addLink(nlr, prediction);
       wf.addLink(split, prediction);


       NodeBinaryClassificationEvaluator bcev = new NodeBinaryClassificationEvaluator(10, "bc evaaluator");
       bcev.labelCol = "label";
       bcev.metricName = "areaUnderROC";
       bcev.rawPredictionCol="rawPrediction";
       wf.addLink(prediction, bcev);

       NodePrintFirstNRows npr = new NodePrintFirstNRows(12, "nprint", 10);
       wf.addLink(bcev, npr);
       wf.execute(jobContext);

}
}