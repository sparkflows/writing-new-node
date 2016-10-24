package fire.examples.workflow.solr;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.solr.NodeSolrQuery;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by manoj on 11/09/16.
 */
public class WorkflowSolrQuery {

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
        // Solr node
        String[] solrcols = {"id_coll"};
        String query = "id_coll:2";

        // solr node
        NodeSolrQuery solr = new NodeSolrQuery(10, "Solr Load Node", collectionName , zkHost  , solrcols  , query);

        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 3 rows", 3);
        wf.addLink(solr, nodePrintFirstNRows);


        // execute the workflow
        wf.execute(jobContext);

    }
}

