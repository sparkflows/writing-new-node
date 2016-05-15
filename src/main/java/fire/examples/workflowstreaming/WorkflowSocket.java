package fire.examples.workflowstreaming;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.context.JobStreamingContextImpl;
import fire.nodes.streaming.NodeStreamingSocketTextStream;
import fire.nodes.streaming.NodeStreamingWordcount;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.WorkflowStreaming;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowSocket {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

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
    private static void socketwf(JobStreamingContextImpl jobStreamingContext) {

        WorkflowStreaming wf = new WorkflowStreaming();

        // socket node
        NodeStreamingSocketTextStream stream = new NodeStreamingSocketTextStream(1, "streaming node");
        wf.addNode(stream);

        // streaming word count
        NodeStreamingWordcount wc = new NodeStreamingWordcount(2, "streaming word count", "message");
        //stream.addNode(wc);
        wf.addLink(stream, wc);

        // execute the workflow
        wf.execute(jobStreamingContext);

    }

}
