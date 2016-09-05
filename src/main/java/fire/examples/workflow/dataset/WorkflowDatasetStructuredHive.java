package fire.examples.workflow.dataset;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowDatasetStructuredHive {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            hivewf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    // read hive table and print some rows
    private static void hivewf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured hive = new NodeDatasetStructured(1, "hive node", "hive:sample_07", DatasetType.HIVE, ",",
                "code description total_emp salary", "string string int int",
                "text text numeric numeric");
        wf.addNode(hive);

        // print first 2 rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(2, "print first rows", 2);
        wf.addLink(hive, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
