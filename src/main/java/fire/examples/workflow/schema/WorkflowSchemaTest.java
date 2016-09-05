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

package fire.examples.workflow.schema;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowSchemaTest {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        schematestwf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void schematestwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "id label f1 f2", "double double double double",
                "numeric numeric numeric numeric");
        csv1.header = false;
        wf.addNode(csv1);

        // vector assembler
        String features = "f1 f2";
        NodeVectorAssembler assembler = new NodeVectorAssembler(5, "feature vector assembler", features,"features_vector");
        wf.addLink(csv1,assembler);

        NodePrintFirstNRows printFirstNRows = new NodePrintFirstNRows(8, "print", 5);
        wf.addLink(csv1, printFirstNRows);

        FireSchema schema = wf.getInputSchema(wf, 5);
        System.out.println(schema.toString());

        schema = wf.getInputSchema(wf, 8);
        System.out.println(schema.toString());

        // execute the workflow
        wf.execute(jobContext);

    }
}
