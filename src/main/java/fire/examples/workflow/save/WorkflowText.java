package fire.examples.workflow.save;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.save.NodeSaveText;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 7/9/2016.
 */
public class WorkflowText {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        text(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void text(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

     /*   NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/churn_prediction/churn.all", DatasetType.CSV, ",", "state account_length area_code phone_number intl_plan voice_mail_plan number_vmail_messages total_day_minutes total_day_calls total_day_charge total_eve_minutes total_eve_calls total_eve_charge total_night_minutes total_night_calls total_night_charge total_intl_minutes total_intl_calls total_intl_charge number_customer_service_calls churn",
                "string double string string string string double double double double double double double double double double double double double double string",
                "text numeric text text text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text");
        wf.addNode(csv1);*/

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");

        wf.addNode(csv1);

        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 10);
        wf.addLink(csv1, nodePrintFirstNRows);

        NodeSaveText nodeSaveText = new NodeSaveText(3, "nst", "data/textOutput");
        wf.addLink(csv1, nodeSaveText);
        nodeSaveText.overwrite = false;

        wf.execute(jobContext);
    }

}
