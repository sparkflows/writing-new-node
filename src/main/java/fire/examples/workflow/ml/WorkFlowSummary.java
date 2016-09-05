package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeHistoGramCal;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 4/17/2016.
 */

public class WorkFlowSummary {

    public static void main(String[] args) throws Exception {
        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executeWfCfAnalysis(jobContext);
        // stop the context
        ctx.stop(); String [] st = new String[]{"count","mean", "min", "25%","50%","75%", "max", "stdev", "variance", "num_zeros", "num_nulls"};

    }


    private static void executeWfCfAnalysis(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

    /*    // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/House_Price_Prediction/kc_house_test_data.csv", DatasetType.CSV, ",",
                "id date price bedrooms bathrooms sqft_living sqft_lot floors waterfront view condition grade sqft_above sqft_basement yr_built yr_renovated zipcode lat long sqft_living15 sqft_lot15",
                "string string int int double double double double double double double double double double double double double double double double double",
                "text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining = "id";
        wf.addNode(csv1);

        NodeSummary nsumm = new NodeSummary(2, "nsumm");
        nsumm.colNames = new String[]{"price", "sqft_living", "sqft_basement"};
        //nsumm.colNames = new String[]{"bedrooms","bathrooms"};
        wf.addLink(csv1, nsumm);*/

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/titanic_dat/train.csv", DatasetType.CSV, ",",
                "PassengerId Survived Pclass Name Sex Age SibSp Parch Ticket Fare Cabin Embarked",
                "int int int string string int int int string double string string",
                "numeric numeric numeric text text numeric numeric numeric text numeric text text ");
        csv1.filterLinesContaining = "PassengerId";
        wf.addNode(csv1);

       /* NodePrintFirstNRows npr = new NodePrintFirstNRows(2, "npr", 100);
        wf.addLink(csv1, npr);*/

     /*   NodeSummary nsumm = new NodeSummary(2, "nsumm");
        nsumm.colNames = new String[]{"PassengerId","Age"};
        wf.addLink(csv1, nsumm);*/

/*        NodeBarChartCal nbcc= new NodeBarChartCal(3, "nbcc");
        nbcc.inputCol ="Pclass";
        wf.addLink(csv1, nbcc);*/

        NodeHistoGramCal nhgc = new NodeHistoGramCal(3, "nhgc");
        nhgc.inputCols="Age";
        nhgc.bins = 8;
        wf.addLink(csv1, nhgc);

        wf.execute(jobContext);

    }
}
