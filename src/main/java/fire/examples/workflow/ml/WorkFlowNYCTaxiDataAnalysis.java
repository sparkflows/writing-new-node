package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeFieldSplitter;
import fire.nodes.etl.NodeSQL;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 5/29/2016.
 */
public class WorkFlowNYCTaxiDataAnalysis {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfNYCTaxiDataAnalysis(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfNYCTaxiDataAnalysis(JobContext jobContext) throws Exception {
        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/trips-subset.csv", DatasetType.CSV, ",",
                "medallion hack_license vendor_id rate_code store_and_fwd_flag pickup_datetime dropoff_datetime passenger_count trip_time_in_secs trip_distance pickup_longitude pickup_latitude dropoff_longitude dropoff_latitude",
                "string string string int string string string int int double double double double double",
                "text text text numeric text text text numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining = "medallion";
        wf.addNode(csv1);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(2, "npr", 10);
        wf.addLink(csv1, npr);

        NodeFieldSplitter nfs = new NodeFieldSplitter(3, "nfs");
        nfs.inputCol= "pickup_datetime";
        nfs.outputCols ="pickup_date,pickup_time";
        nfs.sep =" ";
        wf.addLink(npr, nfs);

        NodeFieldSplitter nfs2 = new NodeFieldSplitter(6, "n");
        nfs2.inputCol= "pickup_time";
        nfs2.outputCols ="pickup_hour,pickup_minute";
        nfs2.sep =":";
        wf.addLink(nfs, nfs2);

        NodeFieldSplitter nfs1 = new NodeFieldSplitter(5, "nfs1");
        nfs1.inputCol = "dropoff_datetime";
        nfs1.outputCols = "dropoff_date,dropoff_time";
        nfs1.sep =" ";
        wf.addLink(nfs2, nfs1);

        NodeFieldSplitter nfs3 = new NodeFieldSplitter(7, "nfs3");
        nfs3.inputCol = "dropoff_time";
        nfs3.outputCols = "drop_hour,drop_minute";
        nfs3.sep =":";
        wf.addLink(nfs1, nfs3);

        NodeCastColumnType ncc = new NodeCastColumnType(9, "ncc");
        String colnames [] = {"pickup_hour", "drop_hour"};
        ncc.colNames = colnames;
        ncc.outputTypes = "int";
        wf.addLink(nfs3, ncc);

        //What is the daily average speed of a taxi?
        NodeSQL sql = new NodeSQL(10, "sql");
        sql.tempTable = "temptable";
        sql.sql = "select temptable.medallion, temptable.pickup_date , temptable.pickup_hour, case when (temptable.trip_time_in_secs = 0 or temptable.trip_distance = 0.0) then 0.0 else (temptable.trip_distance*60*60)/temptable.trip_time_in_secs end as speed from temptable";
        sql.columns = "medallion pickup_date pickup_hour speed";
        sql.columnTypes = "string string int double";
        wf.addLink(nfs3, sql);

        NodePrintFirstNRows npr1 = new NodePrintFirstNRows(8, "npr1", 10);
        wf.addLink(sql, npr1);

        //Filter by a taxi id, 740BD5BE61840BE4FE3905CC3EBE3E7E EA05309C30F375695F44C96108ACB10F

        NodeSQL sql1 = new NodeSQL(11, "sql1");
        sql1.tempTable = "temptable1";
        sql1.sql = "select temptable1.medallion, temptable1.pickup_date, temptable1.pickup_hour, temptable1.speed from temptable1 where medallion in('740BD5BE61840BE4FE3905CC3EBE3E7E','EA05309C30F375695F44C96108ACB10F') order by medallion, pickup_date";
        sql1.columns = "medallion pickup_date pickup_hour speed";
        sql1.columnTypes = "string string int double";
        wf.addLink(npr1, sql1);

        NodePrintFirstNRows npr2 = new NodePrintFirstNRows(12, "npr2", 10);
        wf.addLink(sql1, npr2);

        //Extract average speed in every hour
        NodeSQL sql2 = new NodeSQL(13, "sql2");
        sql2.tempTable = "temptable2";
        sql2.sql = "select temptable2.pickup_hour, avg(temptable2.speed) as avg_speed from temptable2 group by pickup_hour order by pickup_hour";
        sql2.columns = "pickup_hour avg_speed";
        sql2.columnTypes = "int double";
        wf.addLink(npr1, sql2);

        NodePrintFirstNRows npr3 = new NodePrintFirstNRows(14, "npr3", 10);
        wf.addLink(sql2, npr3);

        wf.execute(jobContext);

    }

}
