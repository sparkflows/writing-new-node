package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeUnionDistinct;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sony on 3/13/2016.
 */
public class WorkflowUnion {



    public static void main(String[] args) throws Exception {

        JavaSparkContext ctx = CreateSparkContext.create(args);

        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        unionwf(jobContext);

        ctx.stop();
    }

    public static void unionwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);


        // column filter node
        NodeColumnFilter filter = new NodeColumnFilter(2, "filter node", "id f1 f2");
        wf.addLink(csv1, filter);


        // csv2 node
        NodeDatasetStructured csv2 = new NodeDatasetStructured(3, "csv2 node", "data/rating.csv", DatasetType.CSV, ",",
                "id f5 f6", "double double double",
                "numeric numeric numeric");
        wf.addNode(csv2);


        // UnionNode
        NodeUnionDistinct union= new NodeUnionDistinct(4, "union node");
        wf.addLink(filter, union);
        wf.addLink(csv2, union);

        // print node
        Node print = new NodePrintFirstNRows(5, "print node", 25);
        wf.addLink(union, print);

        wf.execute(jobContext);
    }
}
