package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeUnixTimeConversion;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 6/16/2016.
 */

public class WorkFlowNodeDateConversion {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        dateTimeFiledExtract(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void dateTimeFiledExtract(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/date_extract_sample.dat", DatasetType.CSV, ",",
                "id date_string","string string","text text");
        wf.addNode(csv1);


        NodeUnixTimeConversion nutc = new NodeUnixTimeConversion(2, "nutc");
        nutc.inputCol = "date_string";
        nutc.format ="yyyy-MM-dd HH:mm:ss";
        nutc.value="to_unixtime";
        wf.addLink(csv1, nutc);

        NodePrintFirstNRows npfr = new NodePrintFirstNRows(3, "nprf", 10);
        wf.addLink(nutc, npfr);

        wf.execute(jobContext);
    }

}
