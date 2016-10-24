package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.etl.NodeGeoIP;
import fire.nodes.logs.NodeApacheFileAccessLog;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

public class WorkFlowGeoIP {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            geoip(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void geoip(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        /***
        // structured node
        NodeDatasetStructured structured = new NodeDatasetStructured(1, "Geo Country Node", "data/GeoIP/GeoCountryIP.csv", DatasetType.CSV, ",",
                "IP1 IP2 Country State City", "string string string string string",
                "text text text text text ");
        wf.addNode(structured);
         ***/

        NodeApacheFileAccessLog log = new NodeApacheFileAccessLog(1, "apache log node", "data/apacheaccesslog_new_test.log");
        wf.addNode(log);

        // print n rows
        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(5, "print n rows", 3);
        wf.addLink(log, printFirstNRows);


        String ipCol = "ipAddress";
        NodeGeoIP nodeGeoCountryIP = new NodeGeoIP(10, "Node Geo Country IP", ipCol);
        wf.addLink(printFirstNRows, nodeGeoCountryIP);

        // print n rows
        NodePrintFirstNRows printFirstNRows1 = new NodePrintFirstNRows(15, "print n rows", 6);
        wf.addLink(nodeGeoCountryIP, printFirstNRows1);

        wf.execute(jobContext);

    }
}
