package fire.examples.workflowstreaming;

import fire.context.JobStreamingContextImpl;
import fire.nodes.streaming.NodeStreamingKafka;
import fire.nodes.streaming.NodeStreamingSocketTextStream;
import fire.nodes.streaming.NodeStreamingWordcount;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.FireSchema;
import fire.workflowengine.WorkflowContext;
import fire.workflowenginestreaming.WorkflowStreaming;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowKafka {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark streaming context
        JavaStreamingContext ssc = CreateSparkContext.createStreaming(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobStreamingContextImpl jobContext = new JobStreamingContextImpl(ssc, workflowContext);

        kafkawf(jobContext);

        ssc.start();
        ssc.awaitTermination();
    }


    //--------------------------------------------------------------------------------------

    // kafka workflow
    private static void kafkawf(JobStreamingContextImpl jobStreamingContext) throws Exception {

        WorkflowStreaming wf = new WorkflowStreaming();

        // kafka node
        // set zkhost to the zookeeper node.
        // change 'test' to the topic of interest
        NodeStreamingKafka kafka = new NodeStreamingKafka(1, "kafka node", "zkhost", "consumer-group", "test", 1);
        wf.addNode(kafka);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 50);
        wf.addLink(kafka, nodePrintFirstNRows);

        FireSchema schema = wf.getInputSchema(wf, 2);

        // execute the workflow
        wf.execute(jobStreamingContext);

    }

}
