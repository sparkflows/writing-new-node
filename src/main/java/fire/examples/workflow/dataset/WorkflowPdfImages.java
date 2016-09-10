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

package fire.examples.workflow.dataset;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetPDFImage;
import fire.nodes.util.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowPdfImages {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        pdfwf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void pdfwf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // pdf node
        NodeDatasetPDFImage pdf = new NodeDatasetPDFImage(1, "pdf node", "data/scansmpl.pdf");
        wf.addNode(pdf);

        // print first 1 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(2, "print first 1 row", 1);
        //pdf.addNode(nodePrintFirstNRows);
        wf.addLink(pdf, nodePrintFirstNRows);

        wf.execute(jobContext);
    }

}