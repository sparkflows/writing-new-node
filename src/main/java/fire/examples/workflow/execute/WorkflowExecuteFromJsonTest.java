package fire.examples.workflow.execute;

import fire.execute.ReadFile;
import fire.execute.WorkflowExecuteFromJson;
import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Created by jayant on 7/31/16.
 */
public class WorkflowExecuteFromJsonTest {

    public static void main(String[] args) throws Exception {

        String newargs[] = new String[4];
        //newargs[0] = "local";
        newargs[0] = "localhost:8080";
        newargs[1] = "1";
        String wfjson = ReadFile.readFileFromLocalFilesystem("workflows/kmeans.wf");

        // escape the string
        String escapeFireWorkflow = StringEscapeUtils.escapeJava(wfjson);

        newargs[2] = escapeFireWorkflow;

        WorkflowExecuteFromJson.main(newargs);

    }

}
