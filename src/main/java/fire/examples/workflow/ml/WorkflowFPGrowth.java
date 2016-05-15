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
import fire.nodes.dataset.NodeDatasetTextFiles;
import fire.nodes.ml.NodeFPGrowth;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import fire.context.JobContextImpl;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowFPGrowth {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        fpgrowth(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // fpgrowth workflow
    private static void fpgrowth(JobContext jobContext) {

        Workflow wf = new Workflow();

        // structured node
        NodeDatasetTextFiles txt = new NodeDatasetTextFiles(1, "text node", "data/fpg.txt");

        wf.addNode(txt);

        // fpgrowth node
        NodeFPGrowth fpg = new NodeFPGrowth(2, "fpg node", txt.colName);
        wf.addLink(txt, fpg);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(3, "print first 3 rows", 3);
        wf.addLink(fpg, nodePrintFirstNRows);

        // execute the workflow
        wf.execute(jobContext);

    }

}
