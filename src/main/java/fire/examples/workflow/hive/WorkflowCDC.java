package fire.examples.workflow.hive;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetTextFiles;
import fire.nodes.hive.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by anandmuddebihal on 14/03/16.
 */
public class WorkflowCDC {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        createTable(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void createTable(JobContext jobContext) {

        Workflow wf = new Workflow();

        // text node
        NodeDatasetTextFiles txt = new NodeDatasetTextFiles(1, "text node", "");
        wf.addNode(txt);

        //Node to set hive properties
        NodeHiveProperties hiveProperties = new NodeHiveProperties(2, "set properties");
        wf.addLink(txt, hiveProperties);

        //Node to create external table
        NodeCreateExtTable extTable = new NodeCreateExtTable(3,"create external table");
        wf.addLink(hiveProperties, extTable);

        //Node to create partitioned table 
        NodeCDCTable cdcTable = new NodeCDCTable(4,"create partitioned table");
        wf.addLink(extTable, cdcTable);

        //Node to load data in partitioned table
        NodeLoadTable loadData = new NodeLoadTable(5,"load data");
        wf.addLink(cdcTable, loadData);

        //Node to read the data from table
        NodeReadTable readTable = new NodeReadTable(6, "read data");
        wf.addLink(loadData, readTable);

        // print first 3 rows node 
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(7, "print first 3 rows", 3);
        wf.addLink(readTable, nodePrintFirstNRows);
        wf.execute(jobContext);

    }
}


