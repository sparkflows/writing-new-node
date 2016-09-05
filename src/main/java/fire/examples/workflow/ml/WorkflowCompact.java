package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.compactor.NodeSave;
import fire.nodes.dataset.NodeDatasetTextFiles;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar on 11/12/15.
 */
public class WorkflowCompact {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        textwf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void textwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // text node
        NodeDatasetTextFiles t = new NodeDatasetTextFiles(1, "text node", "data/spam1.csv");
        wf.addNode(t);

        // node compact
        NodeSave compactTextFiles = new NodeSave(2, "compact");
        //t.addNode(compactTextFiles);
        wf.addLink(t, compactTextFiles);

        wf.execute(jobContext);
    }

}
