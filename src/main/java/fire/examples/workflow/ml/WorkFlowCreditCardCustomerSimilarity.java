package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeDateTimeFieldExtract;
import fire.nodes.etl.NodeSQL;
import fire.nodes.ml.NodeKMeans;
import fire.nodes.ml.NodePredict;
import fire.nodes.ml.NodePrintFirstNRows;
import fire.nodes.ml.NodeVectorAssembler;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by tns on 9/15/2016.
 * Finding similar customers based transactions in december-2015. So, that we can recommend the different offers to set of customers.
 */
public class WorkFlowCreditCardCustomerSimilarity {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfCCCSimilarity(jobContext);
        // stop the context
        ctx.stop();
    }

    private static void executewfCCCSimilarity(JobContext jobContext) throws Exception {
        Workflow wf = new Workflow();

        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/cardransactionData.txt", DatasetType.CSV, ",",
                "transactionId cardNumber date amount merchnatId",
                "string string timestamp double string", "yyyy-MM-dd HH:mm:SS",
                "text text text numeric text");
        csv1.filterLinesContaining = "transactionId";
        wf.addNode(csv1);

        //aggregate the transaction at monthly level

        NodeDateTimeFieldExtract ndte = new NodeDateTimeFieldExtract(2, "ndte");
        ndte.inputCol="date";
        ndte.values="year month";
        wf.addLink(csv1, ndte);

        NodeSQL sql = new NodeSQL(3, "sql");
        sql.tempTable = "temptable";
        sql.sql = "select temptable.cardNumber, temptable.date_year, temptable.date_month, sum(amount) as amount, count(*) as transactions from temptable group by temptable.cardNumber, temptable.date_year, temptable.date_month";
        sql.columns = "cardNumber date_year date_month amount transactions";
        sql.columnTypes = "string int int double count";
        wf.addLink(ndte, sql);

        // filter the transactions with 2015-12
        NodeSQL sql1 = new NodeSQL(4, "sql1");
        sql1.tempTable = "temptable1";
        sql1.sql = "select temptable1.cardNumber, temptable1.date_year, temptable1.date_month, temptable1.amount ,temptable1.transactions from temptable1 where date_year=2015 and date_month=12";
        sql1.columns = "cardNumber date_year date_month amount transactions";
        sql1.columnTypes = "string int int double count";
        wf.addLink(sql, sql1);

        NodeVectorAssembler nva = new NodeVectorAssembler(5, "nva");
        nva.inputCols = new String []{"amount","transactions"};
        nva.outputCol = "features";
        wf.addLink(sql1, nva);

        //default value of k is 2, from above analysis it is clear there are 8 different patterns in data.
        NodeKMeans nkm = new NodeKMeans(6, "nkm");
        nkm.k = 2;
        nkm.maxIter = 10;
        nkm.tol = 1.0e-6;
        nkm.featuresCol = "features";
        wf.addLink(nva, nkm);

        NodePredict cluster = new NodePredict(7, "npredict");
        wf.addLink(nkm, cluster);
        wf.addLink(nva, cluster);


        NodePrintFirstNRows npr = new NodePrintFirstNRows(8, "npr", 10);
        wf.addLink(cluster, npr);

        wf.execute(jobContext);
    }
}
