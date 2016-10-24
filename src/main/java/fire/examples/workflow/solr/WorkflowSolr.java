package fire.examples.workflow.solr;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.solr.NodeSolrLoad;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SaveMode;

/**
 * Created by jayant on 11/15/15.
 */

/*
export SPARK_CLASSPATH=$SPARK_CLASSPATH:/mnt/centos/manoj/fire/examples/jar/httpclient-4.5.1.jar:/mnt/centos/manoj/fire/examples/jar/httpcore-4.4.1.jar
spark-submit --class fire.examples.workflow.solr.WorkflowSolr --master yarn-client --executor-memory 1G --num-executors 1 --executor-cores 1 target/fire-examples-1.2.0-jar-with-dependencies.jar ip-172-31-46-4.us-west-2.compute.internal:2181/solr car_collection

curl http://ip-172-31-46-4.us-west-2.compute.internal:8983/solr/car_collection/update?commit=true -H "Content-Type: text/xml" --data-binary '<delete><query>*:*</query></delete>'
solrctl instancedir --generate car_config
vi car_config/conf/schema.xml
<fields>
<field name="id" type="string" indexed="true" stored="true" required="true" multiValued="false" />
<field name="_root_" type="string" indexed="true" stored="false"/>
<field name="label" type="float" indexed="true" stored="true"/>
<field name="f1"  type="float" indexed="true" stored="true"/>
<field name="f2" type="float" indexed="true" stored="true" />
<field name="_version_" type="long" indexed="true" stored="true"/>
</fields>

solrctl collection --delete car_collection
solrctl instancedir --delete car_dir
solrctl instancedir --create car_dir car_config
solrctl collection --create car_collection -c car_dir

*/
public class WorkflowSolr {

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
        NodeSolrLoad solr = new NodeSolrLoad(10, "Solr Load Node", collectionName , zkHost  ,
                                                SaveMode.Overwrite, true , solrcols,  dfcols );
        //csv1.addNode(solr);
        wf.addLink(csv1, solr);

        // execute the workflow
        wf.execute(jobContext);

    }
}
