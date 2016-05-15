package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeBarChartCal;
import fire.nodes.etl.NodeConcatColumns;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/9/2016.
 */
public class WorkFlowDiscreteFeatureAnalysis {

    public static void main(String[] args){
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executeWfCfAnalysis(jobContext);
        // stop the context
        ctx.stop();
    }


    private static void executeWfCfAnalysis(JobContext jobContext) {

        Workflow wf = new Workflow();

        NodeDatasetStructured clicks = new NodeDatasetStructured(1, "csv1 node", "data/ctr_avazu_dataset/train_sample.csv", DatasetType.CSV, ",",
                "id click hour C1 banner_pos site_id site_domain site_category app_id app_domain app_category device_id device_ip device_model device_type device_conn_type C14 C15 C16 C17 C18 C19 C20 C21",
                "string strung string string string string string string string string string string string string string string string string string string string string string string",
                "text text text text text text text text text text text text text text text text text text text text text text text text");

        wf.addNode(clicks);

        NodeConcatColumns nconcat = new NodeConcatColumns(2, "nconcat", "site_id app_id");
        nconcat.outputCol = "site_app_id";
        wf.addLink(clicks, nconcat);

        NodeBarChartCal bcc =  new NodeBarChartCal(5, "bcc");
        bcc.inputCol = "site_app_id";

        wf.addLink(nconcat, bcc);

        wf.execute(jobContext);

    }
}
