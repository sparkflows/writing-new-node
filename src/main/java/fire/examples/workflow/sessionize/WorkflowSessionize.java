package fire.examples.workflow.sessionize;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.logs.NodeApacheFileAccessLog;
import fire.nodes.sessionize.NodeSessionize;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowSessionize {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        execute(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void execute(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // apache file access node
        NodeApacheFileAccessLog log = new NodeApacheFileAccessLog(1, "apache log node", "data/apacheaccesslog_new.log");
        wf.addNode(log);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        wf.addLink(log, nodePrintFirstNRows);

        // sessionize
        NodeSessionize nodeSessionize = new NodeSessionize(3, "sessionize", "ipAddress", "ts");
        wf.addLink(nodePrintFirstNRows, nodeSessionize);

        // execute workflow
        wf.execute(jobContext);
    }

}
