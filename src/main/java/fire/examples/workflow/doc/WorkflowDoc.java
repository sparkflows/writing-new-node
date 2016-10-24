package fire.examples.workflow.doc;

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
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.doc.NodeDocSmall;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import fire.context.JobContextImpl;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowDoc {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        docwf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // doc workflow
    private static void docwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");

        wf.addNode(csv1);

        // doc node
        NodeDocSmall doc1 = new NodeDocSmall(2, "doc", "comment1");
        wf.addNode(doc1);

        // another doc node
        NodeDocSmall doc2 = new NodeDocSmall(3, "doc", "comment2");
        wf.addLink(doc1, doc2);

        // execute the workflow
        wf.execute(jobContext);

    }

}
