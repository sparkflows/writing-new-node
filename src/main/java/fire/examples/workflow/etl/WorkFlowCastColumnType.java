package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by sony on 5/12/2016.
 */
public class WorkFlowCastColumnType {

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            castColumn(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    private static void castColumn(JobContext jobContext) throws Exception{

        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/titanic_dat/train.tsv", DatasetType.CSV, "\t",
                "PassengerId Survived Pclass Name Sex Age SibSp Parch Ticket Fare Cabin Embarked",
                "string string string string string string string string string double string string",
                "text text text text text text text text text numeric text text");

        csv1.filterLinesContaining = "Sex";

        wf.addNode(csv1);

        NodeCastColumnType ncc = new NodeCastColumnType(2, "ncc");
        String [] col = {"Age","PassengerId"};
        ncc.colNames = col;
        ncc.outputTypes = "int";
         wf.addLink(csv1, ncc);

        wf.execute(jobContext);

    }

    }
