package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeFieldSplitter;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 5/26/2016.
 */

public class WorkFlowFieldSplitter {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            fieldSpliter(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void fieldSpliter(JobContext jobContext) throws Exception {


        Workflow wf = new Workflow();

        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/sample.tsv", DatasetType.CSV, "\t",
                "id players team", "int string string",
                "numeric text text");
        wf.addNode(structured);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(2, "npr", 3);
        wf.addLink(structured, npr);

        NodeFieldSplitter nfs = new NodeFieldSplitter(3, "nfs");
        nfs.inputCol= "players";
        nfs.sep ="-";
        nfs.outputCols = "player1,player2,player3";
        wf.addLink(npr, nfs);
        wf.execute(jobContext);



    }

    }
