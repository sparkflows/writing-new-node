package fire.examples.workflow.ml;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetStructured;
import fire.nodes.etl.NodeCastColumnType;
import fire.nodes.etl.NodeColumnFilter;
import fire.nodes.etl.NodeFieldSplitter;
import fire.nodes.etl.NodeSQL;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.DatasetType;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/*
 * Created by tns10 on 6/7/2016.
 */

public class WorkFlowPyisicanPaymentDataAnalysis {

    public static void main(String[] args) throws Exception {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);
        executewfPyisicanPaymentDataAnalysis(jobContext);
        // stop the context
        ctx.stop();
    }

    public static void executewfPyisicanPaymentDataAnalysis(JobContext jobContext) throws Exception{
        Workflow wf = new Workflow();

        //General Payment dataset (dataset with pyisican id's from 1 to 1051)
        NodeDatasetStructured csv1 = new NodeDatasetStructured(1, "csv1 node", "data/Sample_OP_DTL_GNRL_PGYR2013_P01152016.csv", DatasetType.CSV, ",",
                "Covered_Recipient_Type Teaching_Hospital_ID Teaching_Hospital_Name Physician_Profile_ID Physician_First_Name Physician_Middle_Name Physician_Last_Name Physician_Name_Suffix Recipient_Primary_Business_Street_Address_Line1 Recipient_Primary_Business_Street_Address_Line2 Recipient_City Recipient_State Recipient_Zip_Code Recipient_Country Recipient_Province Recipient_Postal_Code Physician_Primary_Type Physician_Specialty Physician_License_State_code1 Physician_License_State_code2 Physician_License_State_code3 Physician_License_State_code4 Physician_License_State_code5 Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country Total_Amount_of_Payment_USDollars Date_of_Payment Number_of_Payments_Included_in_Total_Amount Form_of_Payment_or_Transfer_of_Value Nature_of_Payment_or_Transfer_of_Value City_of_Travel State_of_Travel Country_of_Travel Physician_Ownership_Indicator Third_Party_Payment_Recipient_Indicator Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value Charity_Indicator Third_Party_Equals_Covered_Recipient_Indicator Contextual_Information Delay_in_Publication_Indicator Record_ID Dispute_Status_for_Publication Product_Indicator Name_of_Associated_Covered_Drug_or_Biological1 Name_of_Associated_Covered_Drug_or_Biological2 Name_of_Associated_Covered_Drug_or_Biological3 Name_of_Associated_Covered_Drug_or_Biological4 Name_of_Associated_Covered_Drug_or_Biological5 NDC_of_Associated_Covered_Drug_or_Biological1 NDC_of_Associated_Covered_Drug_or_Biological2 NDC_of_Associated_Covered_Drug_or_Biological3 NDC_of_Associated_Covered_Drug_or_Biological4 NDC_of_Associated_Covered_Drug_or_Biological5 Name_of_Associated_Covered_Device_or_Medical_Supply1 Name_of_Associated_Covered_Device_or_Medical_Supply2 Name_of_Associated_Covered_Device_or_Medical_Supply3 Name_of_Associated_Covered_Device_or_Medical_Supply4 Name_of_Associated_Covered_Device_or_Medical_Supply5 Program_Year Payment_Publication_Date",
                "string string integer string string string string string string string string string string string string string string string string string string string string string string string string string double string integer string string string string string string string string string string string string string string string string string string string string string string string string string string string string string string string string",
                "text text numeric text text text text text text text text text text text text text text text text text text text text text text text text text numeric text numeric text text text text text text text text text text text text text text text text text text text text text text text text text text text text text text text text");
        csv1.filterLinesContaining = "Covered_Recipient_Type";
        wf.addNode(csv1);



       NodePrintFirstNRows npr = new NodePrintFirstNRows(3, "npr", 10);
        wf.addLink(csv1, npr);

        //Pysician Profile dataset

        /*NodeDatasetStructured csv2 = new NodeDatasetStructured(2, "csv2 node", "data/Sample_OP_PH_PRFL_SPLMTL_P01152016.csv", DatasetType.CSV, ",",
                "Physician_Profile_ID Physician_Profile_First_Name Physician_Profile_Middle_Name Physician_Profile_Last_Name Physician_Profile_Suffix Physician_Profile_Alternate_First_Name Physician_Profile_Alternate_Middle_Name Physician_Profile_Alternate_Last_Name Physician_Profile_Alternate_Suffix Physician_Profile_Address_Line_1 Physician_Profile_Address_Line_2 Physician_Profile_City Physician_Profile_State Physician_Profile_Zipcode Physician_Profile_Country_Name Physician_Profile_Province_Name Physician_Profile_Primary_Specialty Physician_Profile_OPS_Taxonomy_1 Physician_Profile_OPS_Taxonomy_2 Physician_Profile_OPS_Taxonomy_3 Physician_Profile_OPS_Taxonomy_4 Physician_Profile_OPS_Taxonomy_5 Physician_Profile_License_State_Code_1 Physician_Profile_License_State_Code_2 Physician_Profile_License_State_Code_3 Physician_Profile_License_State_Code_4 Physician_Profile_License_State_Code_5",
                "integer string string string string string string string string string string string string string string string string string string string string string string string string string string",
                "numeric text text text text text text text text text text text text text text text text text text text text text text text text text text");
        csv2.filterLinesContaining = "Physician_Profile_ID";
        wf.addNode(csv2);

        NodePrintFirstNRows npr = new NodePrintFirstNRows(3, "npr", 10);
        wf.addLink(csv2, npr);*/


        //Number of times & total dollars released for each pyisicans
     /*   NodeSQL sql = new NodeSQL(4, "sql");
        sql.tempTable = "temptable";
        sql.sql = "select temptable.Physician_Profile_ID, temptable.Physician_First_Name, temptable.Physician_Last_Name, count(*) as count, sum(temptable.Total_Amount_of_Payment_USDollars) as total from temptable group by temptable.Physician_Profile_ID, temptable.Physician_First_Name, temptable.Physician_Last_Name order by total desc ";
        sql.columns = "Physician_Profile_ID Physician_First_Name Physician_Last_Name COUNT TOTAL";
        sql.columnTypes = "int string string int double";
        wf.addLink(npr, sql);

        NodePrintFirstNRows npr1 = new NodePrintFirstNRows(5, "npr1", 10);
        wf.addLink(sql, npr1);*/

        // Dollars spend on each month in 2013 ( use graphnode in ui at end)
        NodeColumnFilter  ncf = new NodeColumnFilter(6, "ncf");
        String [] columns = {"Date_of_Payment","Total_Amount_of_Payment_USDollars"};
        ncf.columns = columns;
        wf.addLink(npr, ncf);

        NodeFieldSplitter nfs = new NodeFieldSplitter(7, "nfs");
        nfs.inputCol = "Date_of_Payment";
        nfs.sep = "/";
        nfs.outputCols= "Month,Date,Year";
        wf.addLink(ncf, nfs);

        NodeCastColumnType ncc = new NodeCastColumnType(8, "ncc");
        String [] col = {"Month", "Date", "Year"};
        ncc.colNames = col;
        ncc.outputTypes = "int";
        wf.addLink(nfs, ncc);

        NodeSQL sql = new NodeSQL(9, "sql");
        sql.tempTable = "temptable";
        sql.sql = "select temptable.Month, sum(Total_Amount_of_Payment_USDollars)as Total_Dollar from temptable group by Month order by Month";
        sql.columns = "Month Total_Dollar";
        sql.columnTypes = "int double";
        wf.addLink(ncc, sql);

         NodePrintFirstNRows n=new NodePrintFirstNRows(10, "n", 12);
        wf.addLink(sql, n);

        wf.execute(jobContext);


    }
}
