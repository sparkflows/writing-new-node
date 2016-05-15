package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeReplaceMissingValue;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/2/2016.
 */
public class WorkFlowReplaceMissingValue {


    public static void main(String[] args){
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfReplaceMissingValue(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfReplaceMissingValue(JobContext jobContext){

        Workflow wf = new Workflow();

        NodeDatasetStructured clicks = new NodeDatasetStructured(1, "csv1 node", "data/click_through_rate/dac_sample.txt", DatasetType.CSV, "\t",
                "label IntFeature1 IntFeature2 IntFeature3 IntFeature4 IntFeature5 IntFeature6 IntFeature7 IntFeature8 IntFeature9 IntFeature10 IntFeature11 IntFeature12 IntFeature13 CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26",
                "double double double double double double double double double double double double double double string string string string string string string string string string string string string string string string string string string string string string string string string string",
                "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text text text text text text text text text text text text text text text text text text text text text text text text text text");

        wf.addNode(clicks);

        NodeReplaceMissingValue rmv = new NodeReplaceMissingValue(11, "rmv");
        rmv.colNames = "IntFeature1 IntFeature2 IntFeature3 IntFeature4 IntFeature5 IntFeature6 IntFeature7 IntFeature8 IntFeature9 IntFeature10 IntFeature11 IntFeature12 IntFeature13 CatFeature1 CatFeature2 CatFeature3 CatFeature4 CatFeature5 CatFeature6 CatFeature7 CatFeature8 CatFeature9 CatFeature10 CatFeature11 CatFeature12 CatFeature13 CatFeature14 CatFeature15 CatFeature16 CatFeature17 CatFeature18 CatFeature19 CatFeature20 CatFeature21 CatFeature22 CatFeature23 CatFeature24 CatFeature25 CatFeature26";
        rmv.mode = true;
        wf.addLink(clicks, rmv);


        //NodePrintFirstNRows npr = new NodePrintFirstNRows(12, "nprint", 10);
        //wf.addLink(rmv, npr);

/*        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/missing_data.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);

        NodeReplaceMissingValue rmv = new NodeReplaceMissingValue(11, "rmv");
        rmv.colNames = "id label f1 f2";
        rmv.mode = true;
        wf.addLink(csv1, rmv);


        NodePrintFirstNRows npr = new NodePrintFirstNRows(12, "nprint", 10);
        wf.addLink(rmv, npr);*/
        wf.execute(jobContext);
    }
}
