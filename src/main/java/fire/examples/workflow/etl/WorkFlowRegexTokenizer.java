package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeRegexTokenizer;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

public class WorkFlowRegexTokenizer {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            regexTokenizer(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void regexTokenizer(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/spam.csv", DatasetType.CSV, ",",
                "label message id", "double string double",
                "numeric text numeric");
        wf.addNode(structured);

        NodeRegexTokenizer  nodeRegexTokenizer = new NodeRegexTokenizer(2, "nodeRegexTokenizer", "message", "\\s+", false, "TokenOutput");

        wf.addLink(structured, nodeRegexTokenizer);

        NodePrintFirstNRows nodeRegexTokenizerPrint = new NodePrintFirstNRows(3, "nodeRegexTokenizerPrint", 3);
        wf.addLink(nodeRegexTokenizer, nodeRegexTokenizerPrint);
        wf.execute(jobContext);

    }
}
