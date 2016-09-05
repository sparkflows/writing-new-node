package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;

import fire.nodes.ml.NodeReplaceMissingValueWithConstant;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 5/19/2016.
 */
public class WorkFlowReplaceMisssingValueWithMode {

    public static void main(String[] args) throws Exception {
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfReplaceMissingValue(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfReplaceMissingValue(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/titanic_dat/train.csv", DatasetType.CSV, ",",
                "PassengerId Survived Pclass Name Sex Age SibSp Parch Ticket Fare Cabin Embarked",
                "int int int string string int int int string double string string",
                "numeric numeric numeric text text numeric numeric numeric text numeric text text ");
        csv1.filterLinesContaining = "PassengerId";
        wf.addNode(csv1);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(2, "npr", 30);
        wf.addLink(csv1, npr);

       /* NodeReplaceMissingValueWithMode nrpmv = new NodeReplaceMissingValueWithMode(3, "nrpmv");
        String []col= {"Cabin"};
        nrpmv.colNames = col;
        wf.addLink(npr,nrpmv);

        */

        NodeReplaceMissingValueWithConstant nrpc = new NodeReplaceMissingValueWithConstant(3, "nrpmc");
        String []col= {"Cabin"};
        String [] con = {"S!"};
        nrpc.colNames = col;
        nrpc.constants = con;
        wf.addLink(npr,nrpc);

        NodePrintFirstNRows npr1 = new NodePrintFirstNRows(21, "npr", 30);
        wf.addLink(nrpc, npr1);

        wf.execute(jobContext);


    }
}
