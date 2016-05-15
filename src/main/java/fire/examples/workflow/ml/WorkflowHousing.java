package fire.examples.workflow.ml;

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

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.NodeLinearRegressionWithSGD;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowHousing {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        executewfHousingLinearRegression(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void executewfHousingLinearRegression(JobContext jobContext) {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/housing.csv", DatasetType.CSV, ",",
                "id price lotsize bedrooms bathrms stories driveway recroom fullbase gashw airco garagepl prefarea",
                "string double double double double double string string string string string double string",
                "numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric");
        csv1.filterLinesContaining = "price";

        wf.addNode(csv1);

        // linear regression node
        NodeLinearRegressionWithSGD nodeLinearRegressionWithSGD =
                        new NodeLinearRegressionWithSGD(10, "NodeLinearRegressionWithSGD node", "price", "lotsize bedrooms");

        wf.addLink(csv1, nodeLinearRegressionWithSGD);

        wf.execute(jobContext);
    }

}



