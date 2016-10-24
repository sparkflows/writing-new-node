package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.*;
import fire.nodes.ml.*;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.*;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayant on 4/14/16.
 */
public class WorkflowDedup {
    //--------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        dedup(jobContext);

        // stop the context
        ctx.stop();
    }


    //--------------------------------------------------------------------------------------

    // Dedup
    private static void dedup(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // csv1 node
        NodeDatasetStructured masterData = new NodeDatasetStructured(1, "csv1 node", "data/dedup/rl/masterdata.csv", DatasetType.CSV, ",",
                "first_name last_name gender birth_date ethnicity SSN med_number state city address zip id", "string string string string string string string string string string string string",
                "text text text text text text text text text text text text");
        masterData.filterLinesContaining="id";

        wf.addNode(masterData);

        NodeDatasetStructured errorData = new NodeDatasetStructured(2, "csv1 node", "data/dedup/rl/errordata.csv", DatasetType.CSV, ",",
                "error_first_name error_last_name gender birth_date ethnicity SSN med_number state city address zip error_id", "string string string string string string string string string string string string",
                "text text text text text text text text text text text text");
        errorData.filterLinesContaining="id";

        wf.addNode(errorData);

        NodeJoinUsingColumn joinState = new NodeJoinUsingColumn(3, "join node",  "state");
        // execute the workflow
        wf.addLink(masterData, joinState);
       wf.addLink(errorData, joinState);


        String [] requiredColumns = {"id", "error_id", "state", "first_name", "error_first_name", "last_name", "error_last_name"};
        NodeColumnFilter ncf = new NodeColumnFilter(4, "ncf", requiredColumns);
        wf.addLink(joinState, ncf);


        /*NodeDedup rl = new NodeDedup(5, "ndd" , "first_name", "error_first_name", "levenshtein" , "last_name", "error_last_name", "levenshtein");
        wf.addLink(ncf, rl);*/
        String [] lhs= {"first_name", "last_name"};
        String [] rhs = {"error_first_name", "error_last_name"};
        String [] using = {"levenshtein", "levenshtein"};
        String [] output = {"levenshteinScoreFirstName", "levenshteinScorelastName"};

        NodeDedup rl = new NodeDedup(5, "ndd" , lhs , rhs, using, output);
        wf.addLink(ncf, rl);

        NodeSQL sql = new NodeSQL(6, "node sql");
        sql.tempTable="tempTable";
        sql.sql = "select id, error_id, state as blockingKey, levenshteinScoreFirstName, levenshteinScorelastName, levenshteinScoreFirstName + levenshteinScorelastName as confidenceScore from tempTable  where levenshteinScorelastName > 0.5 and levenshteinScorelastName > 0.6";
        sql.columns= "id error_id blockingKey levenshteinScoreFirstName levenshteinScorelastName confidenceScore";
        sql.columnTypes = "string string string double double double";
        wf.addLink(rl, sql);


        NodeJoinUsingColumns joinGenderEth = new NodeJoinUsingColumns(7, "joinNode",  "gender ethnicity");
        // execute the workflow
        wf.addLink(masterData, joinGenderEth);
        wf.addLink(errorData, joinGenderEth);

        String [] requiredColumns1 = {"id", "error_id", "ethnicity", "gender", "first_name", "error_first_name", "last_name", "error_last_name"};
        NodeColumnFilter ncf1 = new NodeColumnFilter(8, "ncf1", requiredColumns1);
        wf.addLink(joinGenderEth, ncf1);

        /*NodeDedup rl1 = new NodeDedup(9, "ndd" , "first_name", "error_first_name", "levenshtein" , "last_name", "error_last_name", "levenshtein");
        wf.addLink(ncf1, rl1);*/
        String [] lhs1= {"first_name", "last_name"};
        String [] rhs1 = {"error_first_name", "error_last_name"};
        String [] using1 = {"levenshtein", "levenshtein"};
        String [] output1 = {"levenshteinScoreFirstName", "levenshteinScorelastName"};

        NodeDedup rl1 = new NodeDedup(9, "ndd" , lhs1 , rhs1, using1, output1);
        wf.addLink(ncf1, rl1);

        NodeSQL sql1 = new NodeSQL(10, "node sql1");
        sql1.tempTable="tempTable";
        sql1.sql = "select id, error_id, concat( gender,'|',ethnicity ) as blockingKey, levenshteinScoreFirstName, levenshteinScorelastName, levenshteinScoreFirstName + levenshteinScorelastName as confidenceScore from tempTable  where levenshteinScorelastName > 0.5 and levenshteinScorelastName > 0.6";
        sql1.columns= "id error_id blockingKey levenshteinScoreFirstName levenshteinScorelastName confidenceScore";
        sql1.columnTypes = "string string string double double double";
        wf.addLink(rl1, sql1);


        NodeUnionAll nodeUnionAll = new NodeUnionAll(11, "union");
        wf.addLink(sql, nodeUnionAll);
        wf.addLink(sql1, nodeUnionAll);

   /*     //Rank the Confidentiality(most likely)
        NodeSQL sql2 = new NodeSQL(12, "node sql2");
        sql2.tempTable="tempTable";
        sql2.sql = "select id, error_id, confidenceScore, rank() over(partition by id order by confidenceScore desc) as rank from tempTable";
        sql2.columns = "id error_id confidenceScore rank";
        sql2.columnTypes = "string string double int";
        wf.addLink(nodeUnionAll, sql2);*/


        Node print1 = new NodePrintFirstNRows(13, "print node", 300);
        wf.addLink(nodeUnionAll, print1);

        wf.execute(jobContext);

    }

}
