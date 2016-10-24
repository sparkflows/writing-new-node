package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeScala;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 6/22/16.
 */
public class WorkflowScala {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            scalawf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    // scala workflow
    private static void scalawf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(structured);

        // column scala
        NodeScala scala = new NodeScala(2, "scala node");
        scala.code = "inDF.show()\ninDF.show()\ninDF.count()\nval outDF=inDF.drop(\"id\")\noutDF.registerTempTable(\"outDF\")";

        wf.addLink(structured, scala);

        // print first 5 rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(3, "print first rows", 5);
        wf.addLink(scala, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
