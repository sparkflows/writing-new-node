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
import fire.nodes.etl.NodeDropColumns;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by nikhilshekhar
 */
public class WorkflowMultilayerPerceptron {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        perceptronwf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // perceptron workflow
    private static void perceptronwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");

        wf.addNode(csv1);

        NodeVectorAssembler nva = new NodeVectorAssembler(2, "nva");
        nva.inputCols = new String []{"c1","c3"};
        nva.outputCol = "features";
        wf.addLink(csv1, nva);

        NodeStringIndexer labelIndexer = new NodeStringIndexer(3,"label indexer", "c4" ,"label");
        wf.addLink(nva, labelIndexer);

//        //Neural network
        NodeMultilayerPerceptron neuralNetwork = new NodeMultilayerPerceptron(4,"Neural networks");
        neuralNetwork.unitsInLayers = new int[]{2,2,5};
        wf.addLink(labelIndexer,neuralNetwork);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(5, "print first 3 rows", 3);
        wf.addLink(neuralNetwork, nodePrintFirstNRows);

        // execute the workflow

        wf.execute(jobContext);
        
    }
}
