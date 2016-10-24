package fire.examples.workflow.dataset;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetJDBC;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 6/18/16.
 */
public class WorkflowDatasetJDBC {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        jdbcwf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void jdbcwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // text node
        NodeDatasetJDBC jdbc = new NodeDatasetJDBC(1, "jdbc node",
                "jdbc:postgresql:dbserve", "db.table",
                "age name", "int string");
        wf.addNode(jdbc);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        wf.addLink(jdbc, nodePrintFirstNRows);

        wf.execute(jobContext);

    }

}
