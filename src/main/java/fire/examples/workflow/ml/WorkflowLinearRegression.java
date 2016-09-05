/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeMathFuntions;
import fire.nodes.etl.NodeSQL;
import fire.nodes.ml.*;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowLinearRegression {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        lrwf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void lrwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/House_Price_Prediction/kc_house_data.csv", DatasetType.CSV, "\t",
                "id date price bedrooms bathrooms sqft_living sqft_lot floors waterfront view condition grade sqft_above sqft_basement yr_built yr_renovated zipcode lat long sqft_living15 sqft_lot15",
                "string string double double double double double double double double double double double double double double double double double double double",
                "text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining = "id";
        wf.addNode(csv1);


        //Squaring bedrooms will increase the separation between not many bedrooms (e.g. 1) and lots of bedrooms (e.g. 4)
        NodeMathFuntions nmath = new NodeMathFuntions(2, "nmath");
        nmath.inputCol = "bedrooms";
        nmath.mathFunction = "pow";
        wf.addLink(csv1, nmath);

        //Taking the log of squarefeet has the effect of bringing large values closer together and spreading out small values.
        NodeMathFuntions nmat1 = new NodeMathFuntions(3, "namth1");
        nmat1.inputCol = "sqft_living";
        nmat1.mathFunction = "log";
        wf.addLink(nmath, nmat1);

        //bedrooms times bathrooms gives what's called an "interaction" feature. It is large when both of them are large.
        NodeSQL nsql = new NodeSQL(4, "nsql");
        nsql.tempTable = "temp";
        nsql.sql = "select a.*, (a.bedrooms*a.bathrooms) as bedrooms_bathrooms from temp a";
        wf.addLink(nmat1, nsql);


        NodeVectorAssembler nva= new NodeVectorAssembler(5, "nva");
        nva.inputCols = new String[]{"bedrooms_pow2","sqft_living_log", "bedrooms_bathrooms", "floors", "condition", "yr_built","yr_renovated","zipcode","waterfront","view"};
        nva.outputCol ="features";
        wf.addLink(nsql, nva);


        NodeSplit split = new NodeSplit(51, "split");
        split.splitRatio = "0.8 0.2";
        wf.addLink(nva, split);

        NodeLinearRegression nlr = new NodeLinearRegression(6, "nlr");
        nlr.featuresCol = "features";
        nlr.labelCol = "price";
        nlr.elasticNetParam = 1.0;
        nlr.maxIter = 10;
        nlr.fitIntercept = true;
        nlr.standardization = true;
        wf.addLink(split, nlr);

        NodePredict np = new NodePredict(7, "npr");
        wf.addLink(nlr, np);
        wf.addLink(split, np);

        NodeRegressionEvaluator nrev = new NodeRegressionEvaluator(8, "nrev");
        nrev.metricName ="rmse";
        nrev.labelCol = "price";
        nrev.predictionCol = "prediction";
        wf.addLink(np, nrev);

       /* NodePrintFirstNRows npr = new NodePrintFirstNRows(5, "npr", 50);
        wf.addLink(nsql, npr);
         */

        // split node
/*        Node split = new NodeSplit(7, "split node");
        wf.addLink(csv1, split);

        // linear regression node
        NodeLinearRegression regression = new NodeLinearRegression(8, "linear regression node");
        regression.labelCol = "label";
        regression.featuresCol = "f1";
        regression.maxIter = 10;
        regression.regParam = .01;
        wf.addLink(split, regression);

        // score model node
        Node score = new NodePredict(9, "score node");
        wf.addLink(split, score);
        wf.addLink(regression, score);*/

        // execute the workflow
        wf.execute(jobContext);

    }
}
