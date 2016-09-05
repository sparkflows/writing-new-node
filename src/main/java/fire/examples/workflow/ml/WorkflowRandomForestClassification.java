package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 3/20/2016.
 */
public class WorkflowRandomForestClassification {

    public static void main(String [] args) throws Exception {
    // create spark context
    JavaSparkContext ctx = CreateSparkContext.create(args);
    // create workflow context
    WorkflowContext workflowContext = new ConsoleWorkflowContext();
    // create job context
    JobContext jobContext = new JobContextImpl(ctx, workflowContext);

    rfcwf(jobContext);

    // stop the context
    ctx.stop();

    }

    public  static void rfcwf(JobContext jobContext) throws Exception {

        /*
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/covtype.data", DatasetType.CSV, ",",
                "Elevation Aspect Slope Horizontal_Distance_To_Hydrology Vertical_Distance_To_Hydrology Horizontal_Distance_To_Roadways Hillshade_9am Hillshade_Noon Hillshade_3pm Horizontal_Distance_To_Fire_Points Wilderness_Area_1 Wilderness_Area_2 Wilderness_Area_3 Wilderness_Area_4 Soil_Type_1 Soil_Type_2 Soil_Type_3 Soil_Type_4 Soil_Type_5 Soil_Type_6 Soil_Type_7 Soil_Type_8 Soil_Type_9 Soil_Type_10 Soil_Type_11 Soil_Type_12 Soil_Type_13 Soil_Type_14 Soil_Type_15 Soil_Type_16 Soil_Type_17 Soil_Type_18 Soil_Type_19 Soil_Type_20 Soil_Type_21 Soil_Type_22 Soil_Type_23 Soil_Type_24 Soil_Type_25 Soil_Type_26 Soil_Type_27 Soil_Type_28 Soil_Type_29 Soil_Type_30 Soil_Type_31 Soil_Type_32 Soil_Type_33 Soil_Type_34 Soil_Type_35 Soil_Type_36 Soil_Type_37 Soil_Type_38 Soil_Type_39 Soil_Type_40 Cover_Type",
                "double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double",
                "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");

        */

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/titanic_dat/train.tsv", DatasetType.CSV, "\t",
                "PassengerId Survived Pclass Name Sex Age SibSp Parch Ticket Fare Cabin Embarked",
                "string string string string string double string string string double string string",
                "text text text text text numeric text text text numeric text text");

        csv1.filterLinesContaining = "Sex";

        wf.addNode(csv1);

        NodeStringIndexer labelIndexer = new NodeStringIndexer(3,"label indexer", "Survived" ,"label");
        wf.addLink(csv1, labelIndexer);

        String features = "Fare";
        NodeVectorAssembler assembler = new NodeVectorAssembler(4, "feature vector assembler", features,"features_v");
        wf.addLink(labelIndexer,assembler);

        //Build rfc model : pass - feature vector column name and label column name
        NodeRandomForestClassifier rfc = new NodeRandomForestClassifier(6,"random forest","features_v","label");
        wf.addLink(assembler,rfc);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(3, "print first 3 rows", 3);
        wf.addLink(csv1, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
