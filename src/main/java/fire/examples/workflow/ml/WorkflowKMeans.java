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
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDropColumns;
import fire.nodes.ml.NodeKMeans;
import fire.nodes.ml.NodePredict;
import fire.nodes.util.NodePrintFirstNRows;
import fire.nodes.ml.NodeVectorAssembler;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import fire.context.JobContextImpl;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowKMeans {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();

        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        kmeanswf(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // kmeans workflow
    private static void kmeanswf(JobContext jobContext) {

        Workflow wf = new Workflow();

        // structured node
  /*      NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cars.csv", DatasetType.CSV, ",",
                "c1 c2 c3 c4", "double double double double",
                "numeric numeric numeric numeric");

        wf.addNode(csv1);*/

  /*      // kmeans node
        String[] cols = {"c2", "c3"};
        NodeKMeans kMeans = new NodeKMeans(2, "kmeans node", cols);
        kMeans.maxIter = 12;
        kMeans.numClusters = 3;
        wf.addLink(csv1, kMeans);

        // print first 3 rows node
        NodePrintFirstNRows nodePrintFirstNRows = new NodePrintFirstNRows(3, "print first 3 rows", 3);
        wf.addLink(kMeans, nodePrintFirstNRows);*/

        // execute the workflow

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/kdd_cup/kddcup.data", DatasetType.CSV, ",",
                "duration protocol_type service flag src_bytes dst_bytes land wrong_fragment urgent hot num_failed_logins logged_in num_compromised root_shell su_attempted num_root num_file_creations num_shells num_access_files num_outbound_cmds is_host_login is_guest_login count srv_count serror_rate srv_serror_rate rerror_rate srv_rerror_rate same_srv_rate diff_srv_rate srv_diff_host_rate dst_host_count dst_host_srv_count dst_host_same_srv_rate dst_host_diff_srv_rate dst_host_same_src_port_rate dst_host_srv_diff_host_rate dst_host_serror_rate dst_host_srv_serror_rate dst_host_rerror_rate dst_host_srv_rerror_rate reason",
                "double string string string double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double double string",
                "numeric text text text numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric numeric text");

        wf.addNode(csv1);

/*        NodeBarChartCal nbcc = new NodeBarChartCal(2, "nbcc");
        nbcc.inputCols = "reason";
        wf.addLink(csv1, nbcc);*/
        //eight distinct rvalues

        //K-means clustering requires numeric features.
        NodeDropColumns ndc = new NodeDropColumns(3, "ndc", "protocol_type service flag");
        wf.addLink(csv1, ndc);

        NodeVectorAssembler nva = new NodeVectorAssembler(4, "nva");
        nva.inputCols = new String []{"duration","src_bytes","dst_bytes","land","wrong_fragment","urgent","hot","num_failed_logins","logged_in","num_compromised","root_shell","su_attempted","num_root","num_file_creations","num_shells","num_access_files","num_outbound_cmds","is_host_login","is_guest_login","count","srv_count","serror_rate","srv_serror_rate","rerror_rate","srv_rerror_rate","same_srv_rate","diff_srv_rate","srv_diff_host_rate","dst_host_count","dst_host_srv_count","dst_host_same_srv_rate","dst_host_diff_srv_rate","dst_host_same_src_port_rate","dst_host_srv_diff_host_rate","dst_host_serror_rate","dst_host_srv_serror_rate","dst_host_rerror_rate","dst_host_srv_rerror_rate"};
        nva.outputCol = "features";
        wf.addLink(ndc, nva);

        //default value of k is 2, from above analysis it is clear there are 8 different patterns in data.
        NodeKMeans nkm = new NodeKMeans(5, "nkm");
        nkm.k = 12;
        nkm.maxIter = 10;
        nkm.tol = 1.0e-6;
        nkm.featuresCol = "features";
        wf.addLink(nva, nkm);

        NodePredict cluster = new NodePredict(6, "npredict");
        wf.addLink(nkm, cluster);
        wf.addLink(nva, cluster);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(7, "npr", 100);
        wf.addLink(cluster, npr);

        wf.execute(jobContext);
        
    }
}
