package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeGaussianMixture;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 3/8/2016.
 */
public class WorkFlowGaussianMixture {



    public static void main(String[] args){

        JavaSparkContext jsc = CreateSparkContext.create(args);
        WorkflowContext wc = new ConsoleWorkflowContext();

        JobContext jobContext= new JobContextImpl(jsc, wc);

        gaussianwf(jobContext);

        jsc.stop();

    }


    public static void gaussianwf(JobContext jobContext){

        Workflow wf= new Workflow();

        NodeDatasetStructured data = new NodeDatasetStructured(1, "data node", "data/gmm_data.txt", DatasetType.CSV, "\t",
                "c1 c2", "double double",
                "numeric numeric");

        wf.addNode(data);

        NodeGaussianMixture gmm = new NodeGaussianMixture(2,"gmm node");
        wf.addLink(data,gmm);

        wf.execute(jobContext);

    }


}
