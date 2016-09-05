package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDataWrangling;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

public class WorkFlowDataWrangling {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            datawrangling(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void datawrangling(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "csv1 node", "data/spam1.csv", DatasetType.CSV, ",",
                "id message label", "integer string double",
                "numeric text numeric");
        wf.addNode(structured);

        // data wrangling node for column rename rule
        String renameColumnRule = "rename col:id to carsid; rename col:f1 to mpg;";
       //NodeDataWrangling  nodeDataWrangling = new NodeDataWrangling(2, "nodeDataWrangling", renameColumnRule);
       //wf.addLink(structured, nodeDataWrangling);

        // data wrangling node for drop column rule
        String dropColumnRule = "drop col:carsid; drop col:f1;";
        //NodeDataWrangling  nodeDataWrangling = new NodeDataWrangling(2, "nodeDataWrangling", dropColumnRule);
        //wf.addLink(structured, nodeDataWrangling);

        String deleteColumnRule = "delete col:message > 17;";
        NodeDataWrangling  nodeDataWrangling = new NodeDataWrangling(2, "nodeDataWrangling", deleteColumnRule);
        wf.addLink(structured, nodeDataWrangling);

        String subStringColumnRule = "substring col:message 2 4;";
        //NodeDataWrangling  nodeDataWrangling = new NodeDataWrangling(2, "nodeDataWrangling", subStringColumnRule);
        //wf.addLink(structured, nodeDataWrangling);

        // print n rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(5, "print n rows", 3);
        wf.addLink(nodeDataWrangling, printFirstNRows);

        wf.execute(jobContext);

    }
}
