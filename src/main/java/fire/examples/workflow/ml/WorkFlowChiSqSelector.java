package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeChiSqSelector;
import fire.nodes.ml.NodeStringIndexer;
import fire.nodes.ml.NodeVectorAssembler;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 8/15/2016.
 */
public class WorkFlowChiSqSelector {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        chiSqSelector(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // one hot encoder workflow
    private static void chiSqSelector(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);

        NodeStringIndexer nsi = new NodeStringIndexer(10, "nsi", "label", "label_index");
        wf.addLink(csv1, nsi);


        // vector assembler
        String features = "f1 f2";
        NodeVectorAssembler assembler = new NodeVectorAssembler(5, "feature vector assembler", features,"features_vector");
        wf.addLink(nsi,assembler);

        NodeChiSqSelector chiSqSelector = new NodeChiSqSelector(6, "chiSqSelector");
        chiSqSelector.featuresCol="features_vector";
        chiSqSelector.labelCol="label_index";
        chiSqSelector.outputCol="selected_feature";
        chiSqSelector.numTopFeatures=1;
        wf.addLink(assembler, chiSqSelector);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(7, "npr", 3);
        wf.addLink(chiSqSelector, npr);
        wf.execute(jobContext);

    }
}
