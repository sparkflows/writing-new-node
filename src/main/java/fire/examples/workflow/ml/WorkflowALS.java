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
public class WorkflowALS {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        alswf(jobContext);

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    private static void alswf(JobContext jobContext) {

        Workflow wf = new Workflow();

        NodeDatasetStructured ratings = new NodeDatasetStructured(1, "ratings node", "data/movielens/ratings.dat", DatasetType.CSV, "::",
                "UserID MovieID Rating Timestamp", "integer integer double string",
                "numeric numeric numeric text");
        wf.addNode(ratings);

        NodeDatasetStructured movies = new NodeDatasetStructured(2, "movies node", "data/movielens/movies.dat", DatasetType.CSV, "::",
                "MovieID Title Genres", "integer string string",
                "numeric text text");
        wf.addNode(movies);

        NodeSplit split= new NodeSplit(3, "split");
        split.splitRatio="0.8 0.3";
        wf.addLink(ratings, split);

        NodeALS als= new NodeALS(4, "als");
        als.userCol= "UserID";
        als.ratingCol= "Rating";
        als.itemCol= "MovieID";
        als.maxIter = 5;
        als.regParam = 0.1;
        wf.addLink(split, als);

        NodePredict predict = new NodePredict(5, "predict");
        wf.addLink(als, predict);
        wf.addLink(split, predict);

        NodeRegressionEvaluator nre = new NodeRegressionEvaluator(6, "nre");
        nre.metricName="rmse";
        nre.labelCol = "Rating";
        nre.predictionCol = "prediction";
        wf.addLink(predict, nre);
        wf.execute(jobContext);

    }

}

