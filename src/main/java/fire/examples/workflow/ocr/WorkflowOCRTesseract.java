package fire.examples.workflow.ocr;

import fire.context.JobContext;
import fire.context.JobContextImpl;
import fire.nodes.dataset.NodeDatasetBinaryFiles;
import fire.nodes.ocr.NodeOCRTesseract;
import fire.nodes.util.NodePrintFirstNRows;
import fire.spark.CreateSparkContext;
import fire.workflowengine.ConsoleWorkflowContext;
import fire.workflowengine.Workflow;
import fire.workflowengine.WorkflowContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jayantshekhar on 6/15/16.
 */

    // set TESSDATA_PREFIX on mac : launchctl setenv TESSDATA_PREFIX "/Users/jayantshekhar/tessdata"
    // launchctl getenv TESSDATA_PREFIX

public class WorkflowOCRTesseract {

    //--------------------------------------------------------------------------------------

    public static void main(String[] args) {

        // create spark context
        JavaSparkContext ctx = CreateSparkContext.create(args);
        // create workflow context
        WorkflowContext workflowContext = new ConsoleWorkflowContext();
        // create job context
        JobContext jobContext = new JobContextImpl(ctx, workflowContext);

        try {
            ocr(jobContext);
        } catch(Exception ex) {
            ex.printStackTrace();
        }

        // stop the context
        ctx.stop();
    }

    //--------------------------------------------------------------------------------------

    // ocr tesseract workflow
    private static void ocr(JobContext jobContext) throws Exception {

        Workflow wf = new Workflow();

        // pdf image node
        //NodeDatasetPDFImageOCR pdfImage = new NodeDatasetPDFImageOCR(1, "pdf image node", "data/scansmpl.pdf");
        //wf.addNode(pdfImage);

        NodeDatasetBinaryFiles binaryFiles = new NodeDatasetBinaryFiles(1, "binary file", "data/ocrimage.png");
        wf.addNode(binaryFiles);

        // ocr tesseract node
        NodeOCRTesseract tesseract = new NodeOCRTesseract(5, "ocr node", "file", "content");
        wf.addLink(binaryFiles, tesseract);

        // print first 2 rows
        NodePrintFirstNRows printFirstNRows5 = new NodePrintFirstNRows(10, "print first rows", 20);
        wf.addLink(tesseract, printFirstNRows5);

        // execute the workflow
        wf.execute(jobContext);

    }

}


