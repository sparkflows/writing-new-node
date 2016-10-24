package fire.examples.workflow.logs;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.logs.NodeApacheErrorLog;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sumedh on 25/06/16.
 */
public class WorkflowApacheErrorLog {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        apachelogwf(jobContext);

        // stop the context
        ctx.stop();
    }

    private static void apachelogwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // error node
        NodeApacheErrorLog log1 = new NodeApacheErrorLog(1, "apache error node", "data/apacheerrorlog.log");
        wf.addNode(log1);

        // print first row node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 1 row", 1);
        //tika.addNode(nodePrintFirstNRows);
        wf.addLink(log1, nodePrintFirstNRows);

        wf.execute(jobContext);
    }
}
