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
import fire.nodes.ml.*;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowLogisticRegression {

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
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        wf.addNode(csv1);

        NodeStringIndexer nsi = new NodeStringIndexer(10, "nsi", "label", "label_index");
        wf.addLink(csv1, nsi);


        // vector assembler
        String features = "f1 f2";
        NodeVectorAssembler assembler = new NodeVectorAssembler(5, "feature vector assembler", features,"features_vector");
        wf.addLink(nsi,assembler);

        // split node
     /*   NodeSplit split = new NodeSplit(7, "split node");
        split.fraction1 = .9;
        wf.addLink(assembler, split);*/

        // logistic regression node
        NodeLogisticRegression regression = new NodeLogisticRegression(8, "regression node");
        regression.labelCol = "label_index";
        regression.featuresCol = "features_vector";
        regression.maxIter = 10;
        regression.regParam = .01;
        regression.elasticNetParam=0.0;
        regression.regParam = 0.0;
        regression.fitIntercept= true;
        regression.standardization= true;
        regression.tol=1E-6;

        wf.addLink(assembler, regression);

        // score model node
        Node predict = new NodePredict(9, "predict node");
        //wf.addLink(assembler, predict);
        wf.addLink(regression, predict);

        NodeBinaryClassificationEvaluator nbev= new NodeBinaryClassificationEvaluator(11, "bvev");
        nbev.labelCol="label_index";
        nbev.metricName="areaUnderROC";
        nbev.rawPredictionCol="rawPrediction";
        wf.addLink(predict, nbev);

        /*NodePrintFirstNRows npf = new NodePrintFirstNRows(12, "npf", 1);
        wf.addLink(predict, npf);*/

        // execute the workflow
        wf.execute(jobContext);

    }
}
