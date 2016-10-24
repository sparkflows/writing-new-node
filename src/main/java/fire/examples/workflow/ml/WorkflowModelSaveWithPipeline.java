package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 7/24/2016.
 */
public class WorkflowModelSaveWithPipeline {


    public static void main(String[] args)throws Exception{
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        modelsavewf(jobContext);

        // stop the context
        ctx.stop();
    }

    private static void modelsavewf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();
        // execute the workflow

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/kdd_cup/kddcup.data", DatasetType.CSV, ",",
                "duration protocol_type service flag src_bytes dst_bytes land wrong_fragment urgent hot num_failed_logins logged_in num_compromised root_shell su_attempted num_root num_file_creations num_shells num_access_files num_outbound_cmds is_host_login is_guest_login count srv_count serror_rate srv_serror_rate rerror_rate srv_rerror_rate same_srv_rate diff_srv_rate srv_diff_host_rate dst_host_count dst_host_srv_count dst_host_same_srv_rate dst_host_diff_srv_rate dst_host_same_src_port_rate dst_host_srv_diff_host_rate dst_host_serror_rate dst_host_srv_serror_rate dst_host_rerror_rate dst_host_srv_rerror_rate reason",
                "double string string string double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double string",
                "numeric text text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text");

        wf.addNode(csv1);

        NodeVectorAssembler nva = new NodeVectorAssembler(4, "nva");
        nva.inputCols = new String []{"duration","src_bytes","dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins","logged_in","num_compromised","root_shell","su_attempted","num_root","num_file_creations","num_shells","num_access_files","num_outbound_cmds","is_host_login","is_guest_login","count","srv_count","serror_rate","srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate","diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count","dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate","dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate","dst_host_rerror_rate","dst_host_srv_rerror_rate"};
        nva.outputCol = "features";
        wf.addLink(csv1, nva);

        //default value of k is 2, from above analysis it is clear there are 8 different patterns in data.
        NodeKMeans nkm = new NodeKMeans(5, "nkm");
        nkm.k = 12;
        nkm.maxIter = 10;
        nkm.tol = 1.0e-6;
        nkm.featuresCol = "features";
        wf.addLink(nva, nkm);

        NodePipeline pipeline = new NodePipeline(6, "pipeline");
        wf.addLink(nkm, pipeline);


        /*NodeModelSave save = new NodeModelSave(7, "model save");
        save.path = "modelsave";
        save.overwrite = true;
        wf.addLink(pipeline, save);*/

        /*NodePMMLExport pmml = new NodePMMLExport(7, "pmml");
        pmml.path= "LogisticregressionCars.pmml";
        wf.addLink(pipeline, pmml);*/


        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(9, "print first 3 rows", 3);
        wf.addLink(pipeline, nodePrintFirstNRows);

    }
}
