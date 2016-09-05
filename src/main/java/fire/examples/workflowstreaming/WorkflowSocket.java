package fire.examples.workflowstreaming;

import fire.context.JobStreamingContextImpl;
import fire.nodes.streaming.NodeStreamingSocketTextStream;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.WorkflowStreaming;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowSocket {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark streaming context
        JavaStreamingContext ssc = CreateSparkContext.createStreaming(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobStreamingContextImpl jobContext = new JobStreamingContextImpl(ssc, workflowContext);

        socketwf(jobContext);

        ssc.start();
        ssc.awaitTermination();
    }


    //--------------------------------------------------------------------------------------

    // socket workflow
    private static void socketwf(JobStreamingContextImpl jobStreamingContext) throws Exception {

        WorkflowStreaming wf = new WorkflowStreaming();

        // socket node
        NodeStreamingSocketTextStream stream = new NodeStreamingSocketTextStream(1, "streaming node");
        wf.addNode(stream);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 50);
        wf.addLink(stream, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobStreamingContext);

    }

}
