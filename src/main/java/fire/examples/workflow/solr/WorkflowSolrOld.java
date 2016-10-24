package fire.examples.workflow.solr;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.solr.NodeSolrLoadOld;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 11/15/15.
 */
public class WorkflowSolrOld {

    static String zkHost = "localhost:9983";
    static String collectionName = "load_collection";
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
            if(args.length==2)
            {
            zkHost = args[0];
            collectionName = args[1];
            }
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        solrwf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // solr workflow
    private static void solrwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);

        // Solr node
        String[] dfcols = {"id", "label", "f1", "f2"};
        String[] solrcols = {"id", "label", "f1", "f2"};

        // solr node
        NodeSolrLoadOld solr = new NodeSolrLoadOld(10, "Solr Load Node", collectionName , zkHost  , solrcols,  dfcols );
        //csv1.addNode(solr);
        wf.addLink(csv1, solr);

        // execute the workflow
        wf.execute(jobContext);

    }
}
