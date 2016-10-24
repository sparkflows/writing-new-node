package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeSQLTransformer;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

public class WorkflowSQLTransformer {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        wf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // HBase workflow
    private static void wf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // CSV node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);

        // sql transformer node
        String cols = "c1 c2 c3 c4 v1 v2";
        String colTypes = "double double double double double double";
        NodeSQLTransformer sqlTransformer = new NodeSQLTransformer(10, "sqltransformer node", cols, colTypes);
        sqlTransformer.sql = "SELECT *, (c1 + c2) AS v3, (c1 * c2) AS v4 FROM __THIS__";
        wf.addLink(csv1, sqlTransformer);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(16, "print first 3 rows", 3);
        wf.addLink(sqlTransformer, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
