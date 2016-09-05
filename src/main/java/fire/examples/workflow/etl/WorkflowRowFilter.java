package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeRowFilter;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by anandmuddebihal on 24/03/16.
 */
public class WorkflowRowFilter {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            filterwf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    // filter columns workflow workflow
    private static void filterwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(structured);

        // column filter node
        NodeRowFilter filter = new NodeRowFilter(2, "filter node", "id > 3");
        wf.addLink(structured, filter);

        // print first 10 rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(3, "print first rows", 10);
        wf.addLink(filter, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }
}
