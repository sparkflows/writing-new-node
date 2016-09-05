package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.ml.*;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 3/20/2016.
 */
public class WorkFlowChurnPrediction  {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfChurnPrediction(jobContext);
        // stop the context
        ctx.stop();
    }


    private static void executewfChurnPrediction(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/churn_prediction/churn.all", DatasetType.CSV, ",", "state account_length area_code phone_number intl_plan voice_mail_plan number_vmail_messages total_day_minutes total_day_calls total_day_charge total_eve_minutes total_eve_calls total_eve_charge total_night_minutes total_night_calls total_night_charge total_intl_minutes total_intl_calls total_intl_charge number_customer_service_calls churn",
                "string double string string string string double double double double double double double double double double double double double double string",
                "text numeric text text text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text");
        wf.addNode(csv1);

        //feature engineering
        NodeStringIndexer planindex= new NodeStringIndexer(2, "plan indexer", "intl_plan", "intl_plan_indexed");
        wf.addLink(csv1, planindex);

        NodeStringIndexer  labelIndexer = new NodeStringIndexer(3,"label indexer", "churn" ,"label");
        wf.addLink(planindex, labelIndexer);

        //select features
        String  features = "account_length number_vmail_messages total_day_calls total_day_charge total_eve_calls total_eve_charge total_night_calls total_intl_calls total_intl_charge intl_plan_indexed";

        NodeVectorAssembler assembler = new NodeVectorAssembler(4, "feature vector assembler", features,"features_v");
        wf.addLink(labelIndexer,assembler);

        Node split = new NodeSplit(5, "split node");
        wf.addLink(assembler, split);

        //Build rfc model : pass - feature vector column name and label column name
        NodeRandomForestClassifier rfc = new NodeRandomForestClassifier(6,"random forest","features_v","label");
        rfc.impurity="gini";
        rfc.maxBins=32;
        rfc.maxDepth=2;
        rfc.maxMemoryInMB= 256;
        rfc.minInstancesPerNode=1;
        rfc.numTrees=20;
        rfc.minInfoGain= 0.0;
        rfc.subsamplingRate= 1.0;
        rfc.checkpointInterval=10;
        rfc.cacheNodeIds=false;
        rfc.featureSubsetStrategy= "auto";
        wf.addLink(split,rfc);

        //TODO:tns add cross-validation
        NodePredict prediction = new NodePredict(7,"predict");
        wf.addLink(rfc, prediction);
        wf.addLink(split, prediction);

        NodeBinaryClassificationEvaluator bcev = new NodeBinaryClassificationEvaluator(8, "bc evaaluator", "label" ,"areaUnderROC");
        bcev.rawPredictionCol="rawPrediction";
        wf.addLink(prediction, bcev);

       // NodeColumnFilter ncf = new NodeColumnFilter(9, "column filter", "state area_code phone_number label prediction");

        //wf.addLink(bcev, ncf);
        wf.execute(jobContext);

    }
}
