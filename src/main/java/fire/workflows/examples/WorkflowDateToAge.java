package fire.workflows.examples;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDateToAge;
import fire.nodes.etl.NodeDateToString;
import fire.nodes.etl.NodeStringToDate;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 3/9/2017.
 */
public class WorkflowDateToAge {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        dateToAge(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void dateToAge(JobContext jobContext) throws Exception {


        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/date_extract_sample.dat", DatasetType.CSV, ",",
                "id date_string","string string","text text");
        wf.addNode(csv1);


        NodeStringToDate nutc = new NodeStringToDate(2, "nutc");
        nutc.setInputColName("date_string");
        nutc.setInputColFormat("y-M-d HH:mm:ss");
        nutc.setOutputColName("DateCol");
        nutc.setOutputColType(FireSchema.Type.DATE);
        wf.addLink(csv1, nutc);


        NodeDateToAge ndts = new NodeDateToAge(3, "ndts");
        ndts.setInputColName("DateCol");
        ndts.setYearsOutputColName("NumYears");
        ndts.setDaysOutputColName("NumDays");
        wf.addLink(nutc, ndts);


        NodePrintFirstNRows npfr = new NodePrintFirstNRows(4, "nprf", 100);
        wf.addLink(ndts, npfr);

        wf.execute(jobContext);
    }

}


