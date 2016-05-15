package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeNumericImputer;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/11/2016.
 */
public class WorkFlowNumericImputer {

    public static void main(String[] args){
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfNumericImputer(jobContext);
        // stop the context
        ctx.stop();
    }


    private static void executewfNumericImputer(JobContext jobContext) {

        Workflow wf = new Workflow();

        NodeDatasetStructured titanic = new NodeDatasetStructured(1, "csv1 node", "data/titanic_dat/train.csv", DatasetType.CSV, "\t",
                "PassengerId Survived Pclass Name Sex Age SibSp Parch Ticket Fare Cabin Embarked",
                "int int int string string int int int string double string string",
                "numeric numeric numeric text text numeric numeric numeric text numeric text text");

        titanic.filterLinesContaining = "Sex";
        wf.addNode(titanic);

        // agg has null's;
        NodeNumericImputer numericImputer = new NodeNumericImputer(2, "numericImputer", "Age");

        wf.addLink(titanic, numericImputer);
        wf.execute(jobContext);

    }
}
