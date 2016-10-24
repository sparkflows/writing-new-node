package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDateTimeFieldExtract;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 6/13/2016.
 */
public class WorkFlowNodeDateTimeFiledExtract {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        dateTimeFiledExtract(jobContext);

        // stop the context
        ctx.stop();
    }

    private static void dateTimeFiledExtract(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/date_extract_sample.dat", DatasetType.CSV, ",",
               "id date","string timestamp","yyyy-MM-dd HH:mm:SS","text text");
        wf.addNode(csv1);

        NodeDateTimeFieldExtract ndte = new NodeDateTimeFieldExtract(2, "ndte");
        ndte.inputCol="date";
        //ndte.datetime_format="yyyy-MM-dd HH:mm:SS";
        ndte.values="year month hour";

        wf.addLink(csv1, ndte);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(3,"npr", 10);
        wf.addLink(ndte, npr);

        wf.execute(jobContext);

    }

    }
