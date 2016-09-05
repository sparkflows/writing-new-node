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
import fire.nodes.etl.NodeJoinUsingSQL;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeColumnsRename;
import fire.nodes.etl.NodeSQL;
import fire.nodes.ml.*;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.util.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar
 */
public class WorkflowALS {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

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

    private static void alswf(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        NodeDatasetStructured ratings = new NodeDatasetStructured(1, "ratings node", "data/movielens/ratings_colon_seperator.dat", DatasetType.CSV, ":",
                "UserID MovieID Rating Timestamp", "integer integer double string",
                "numeric numeric numeric text");
        wf.addNode(ratings);

    /*    NodeDatasetStructured movies = new NodeDatasetStructured(2, "movies node", "data/movielens/movies.dat", DatasetType.CSV, "::",
                "MovieID Title Genres", "integer string string",
                "numeric text text");
        wf.addNode(movies);*/

        //
        NodeSQL sql = new NodeSQL(11, "node sql");
        sql.tempTable="als_table";
        sql.sql = "select als_table.UserID, count(*) as count from als_table group by als_table.UserID";
        sql.columns= "UserID COUNT";
        sql.columnTypes = "int int";
        wf.addLink(ratings, sql);

        NodeSQL sql1 = new NodeSQL(12, "node sql1");
        sql1.tempTable="als_table1";
        sql1.sql = "select als_table1.UserID from als_table1 where als_table1.UserID > 50";
        sql1.columns= "UserID";
        sql1.columnTypes = "int";
        wf.addLink(sql, sql1);


        NodeJoinUsingSQL join = new NodeJoinUsingSQL(13, "join");
     /*   join.joinType= "INNER";
        join.leftCols = "UserID";
        join.rightCols = "UserID";*/
        join.tempTable1="fire_temp_table1";
        join.tempTable2="fire_temp_table2";
        join.sql="select a.UserID as left_UserID, a.MovieID as left_MovieID, a.Rating as left_Rating from fire_temp_table1 a JOIN fire_temp_table2 b ON(a.UserID=b.UserID)";
        join.columns="left_UserID left_MovieID left_Rating";
        join.columnTypes="integer integer double";
        wf.addLink(ratings, join);
        wf.addLink(sql1, join);

        NodeColumnFilter columnFilter = new NodeColumnFilter(14, "ncf");
        String [] columns = {"left_UserID","left_MovieID","left_Rating"};
        columnFilter.columns= columns;
        wf.addLink(join, columnFilter);

        NodeColumnsRename ncr = new NodeColumnsRename(15, "ncr");
        String [] colCurrName = {"left_UserID","left_MovieID","left_Rating"};
        String [] colNewName = {"UserID","MovieID","Rating"};
        ncr.colCurrName = colCurrName;
        ncr.colNewName = colNewName;
        wf.addLink(columnFilter, ncr);

        NodeSplit split= new NodeSplit(3, "split");
        split.splitRatio="0.8 0.2";
        wf.addLink(ncr, split);

        NodeALS als= new NodeALS(4, "als");
        als.userCol= "UserID";
        als.ratingCol= "Rating";
        als.itemCol= "MovieID";
        als.maxIter = 5;
        als.regParam = 0.01;
        als.rank = 10;
        wf.addLink(split, als);

        NodePredict predict = new NodePredict(5, "predict");
        wf.addLink(als, predict);
        wf.addLink(split, predict);

        NodeSQL sql2 = new NodeSQL(6, "sql2");
        sql2.tempTable="temp";
        sql2.sql="select temp.* from temp where prediction >=3.0";
        wf.addLink(predict, sql2);

        NodePrintFirstNRows npfr = new NodePrintFirstNRows(7, "npfr", 50);
        wf.addLink(sql2, npfr);

        NodeRegressionEvaluator nre = new NodeRegressionEvaluator(8, "nre");
        nre.metricName= "rmse";
        nre.labelCol = "Rating";
        nre.predictionCol = "prediction";
        wf.addLink(npfr, nre);

        wf.execute(jobContext);

    }

}