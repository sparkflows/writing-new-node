package fire.examples.workflow.etl;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns10 on 3/19/2016.
 */
public class WorkFlowMovieLenseData {


    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        joinwf(jobContext);

        // stop the context
        ctx.stop();
    }

    private static void joinwf(JobContext jobContext) throws Exception {

      /*  Workflow wf = new Workflow();

        NodeDatasetStructured ratings = new NodeDatasetStructured(1, "ratings node", "data/movielens/ratings.dat", DatasetType.CSV, "::",
                "UserID MovieID Rating Timestamp", "integer integer double string",
                "numeric numeric numeric text");
        wf.addNode(ratings);

        NodeDatasetStructured users = new NodeDatasetStructured(2, "users node", "data/movielens/users.dat", DatasetType.CSV, "::",
                "UserID Gender Age Occupation ZipCode", "integer string integer integer string",
                "numeric categorical numeric numeric text");
        wf.addNode(users);

        // join node
        NodeAllJoin join = new NodeAllJoin(3, "join node", "inner","UserID","UserID");
        wf.addLink(ratings, join);
        wf.addLink(users, join);


        NodeDatasetStructured movies = new NodeDatasetStructured(4, "movies node", "data/movielens/movies.dat", DatasetType.CSV, "::",
                "MovieID Title Genres", "integer string string",
                "numeric text text");

        //Schema of join : left_UserID left_MovieID left_Rating left_Timestamp right_UserID right_Gender right_Age right_Occupation right_ZipCode
        NodeAllJoin join1 = new NodeAllJoin(5, "join node1", "inner","left_MovieID","MovieID");
        wf.addLink(join, join1);
        wf.addLink(movies, join1);

        NodeDatasetStructured occupation = new NodeDatasetStructured(6, "movies node", "data/movielens/occupation.dat", DatasetType.CSV, ":",
                "OccupationId OccupationName", "integer string",
                "numeric text");


        //Schema of join1 : left_left_UserID left_left_MovieID left_left_Rating left_left_Timestamp left_right_UserID left_right_Gender left_right_Age left_right_Occupation left_right_ZipCode right_MovieID right_Title right_Genres
        NodeAllJoin join2 = new NodeAllJoin(7, "join node2", "right","left_right_Occupation","OccupationId");
        wf.addLink(join1, join2);
        wf.addLink(occupation, join2);


        // Schema of join2:left_left_left_UserID left_left_left_MovieID left_left_left_Rating left_left_left_Timestamp left_left_right_UserID left_left_right_Gender left_left_right_Age left_left_right_Occupation left_left_right_ZipCode left_right_MovieID left_right_Title left_right_Genres right_OccupationId right_OccupationName

        NodeColumnFilter filter = new NodeColumnFilter(8, "filter node", "left_left_left_UserID left_left_left_MovieID left_left_left_Rating left_left_left_Timestamp left_left_right_Gender left_left_right_Age left_left_right_ZipCode left_right_Title left_right_Genres right_OccupationId right_OccupationName");
        wf.addLink(join2, filter);

        NodeColumnsRename rename = new NodeColumnsRename(9,"rename node",
                "UserID MovieID Rating Timestamp Gender Age ZipCode Title Genres OccupationId OccupationName",
                "UserID MovieID Rating Timestamp Gender Age ZipCode Title Genres OccupationId OccupationName");
        wf.addLink(filter, rename);

        //print node
        Node print = new NodePrintFirstNRows(10, "print node", 10);
        wf.addLink(rename, print);

        // summary statistics node
     *//*   Node summaryStatistics = new NodeSummaryStatistics(5, "summary statistics");
        wf.addLink(print, summaryStatistics);*//*

        wf.execute(jobContext);*/
    }

}
