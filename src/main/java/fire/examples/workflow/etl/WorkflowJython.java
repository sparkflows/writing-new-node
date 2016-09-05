package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeJython;
import fire.nodes.save.NodeSaveParquet;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.hdfsio.Delete;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 6/22/16.
 */
public class WorkflowJython {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            jythonwf(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    // filter columns workflow workflow
    private static void jythonwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(structured);

        // column jython
        NodeJython jython = new NodeJython(2, "jython node");
        jython.code = "df = inDF.drop(\"f1\")";
        jython.code = "from org.apache.spark.sql import Row\nfor r in inDF.collect():\n  print(r)\noutDF = inDF";
        jython.code = "inDF.javaRDD().map(lambda p: p.f1)\n" +
                        "outDF = inDF";

        wf.addLink(structured, jython);

        // print first 5 rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(3, "print first rows", 5);
        wf.addLink(jython, printFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
