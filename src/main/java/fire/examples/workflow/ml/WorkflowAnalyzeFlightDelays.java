package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeSQL;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.nodes.ml.NodeStringIndexer;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 5/21/2016.
 */
public class WorkflowAnalyzeFlightDelays {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfAnalyzeFlightDelays(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfAnalyzeFlightDelays(JobContext jobContext) throws Exception {
        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/flights_data.csv", DatasetType.CSV, ",",
                "DAY_OF_MONTH DAY_OF_WEEK CARRIER TAIL_NUM FL_NUM ORIGIN_AIRPORT_ID ORIGIN DEST_AIRPORT_ID DEST CRS_DEP_TIME DEP_TIME DEP_DELAY_NEW CRS_ARR_TIME ARR_TIME ARR_DELAY_NEW CRS_ELAPSED_TIME DISTANCE",
                "integer integer string string integer integer string integer string integer integer integer integer integer integer integer integer",
                "numeric numeric text text numeric numeric text numeric text numeric numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining="DEST";
        wf.addNode(csv1);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(2, "npr", 50);
        wf.addLink(csv1, npr);

        NodeCastColumnType ncct = new NodeCastColumnType(3, "ncct");
        String [] columns = {"CRS_DEP_TIME","CRS_ARR_TIME","CRS_ELAPSED_TIME"};
        ncct.colNames = columns;
        ncct.outputTypes = "double";
        wf.addLink(npr, ncct);

        NodeCastColumnType ncct1 = new NodeCastColumnType(31, "ncct1");
        String [] columns1 = {"DAY_OF_MONTH","DAY_OF_WEEK"};
        ncct1.colNames = columns1;
        ncct1.outputTypes = "string";
        wf.addLink(ncct, ncct1);


        NodeStringIndexer nsi = new NodeStringIndexer(4, "nsi");
        String [] columns2 = {"DAY_OF_MONTH", "DAY_OF_WEEK", "CARRIER", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID"};
        String [] outputco2 = {"DAY_OF_MONTH_INDEX", "DAY_OF_WEEK_INDEX", "CARRIER_INDEX", "ORIGIN_AIRPORT_ID_INDEX", "DEST_AIRPORT_ID_INDEX"};
        nsi.inputCols = columns2;
        nsi.outputCols = outputco2;
        wf.addLink(ncct1 , nsi);

        NodePrintFirstNRows npr1 = new NodePrintFirstNRows(5, "npr", 50);
        wf.addLink(nsi, npr1);

        NodeSQL sql = new NodeSQL(6, "sql");
        sql.tempTable = "temptable";
        sql.sql = "select temptable.* , case  when temptable.DEP_DELAY_NEW > 40 then 1.0 else 0.0 END as label from temptable";
        wf.addLink(npr1, sql);

        NodePrintFirstNRows npr2 = new NodePrintFirstNRows(7, "npr", 50);
        wf.addLink(sql, npr2);

        wf.execute(jobContext);
    }
}
