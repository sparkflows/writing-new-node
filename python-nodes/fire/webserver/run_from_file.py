import argparse
import subprocess
import sys
import json

from fire.webserver.run_wf import *
from fire.nodes.read_wf_json import *
from fire.workflowcontext import *

"""Reads the workflow file from hdfs or from the local file system"""


def read_file(fn: str):

    f_content = ""

    if fn.startswith("hdfs"):
        print("hdfs")

        cat = subprocess.Popen(
            ["hadoop", "fs", "-cat", fn], stdout=subprocess.PIPE)
        for line in cat.stdout:
            ll = line.decode('utf-8')
            f_content += ll

        return f_content

    with open(fn, "r") as workflow_file:
        f_content = workflow_file.read()

    return f_content


"""Executes the given workflow which has been created by reading from file"""


def run_workflow_created_from_file(jobContext: JobContext, workflow: Workflow):

    jobContext.outSparkContext()

    workflow.setNumIncomingConnections()
    workflow.setNextNodes()

    startingNodes = workflow.startingNodesToExecuteForExecuteTill(0)

    for node in startingNodes:
        if jobContext.stopExecution:
            return

        print(node)
        node.printExecutingNode(jobContext)
        node.execute(jobContext)

    output_success: OutputSuccess = OutputSuccess(
        99999,
        "name",
        "name",
        "Successfully finished executing workflow",
        resultType=3,
        visibility="EXPANDED")
    jobContext.workflowContext.outSuccess(output_success)

    return


"""Reads the workflow json from the given file and executes it"""


def run_workflow_from_file(
        cluster: str,
        fn: str,
        postback_url: str,
        job_id: str):
    print(str)

    #json_path = os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)), fn)

    f_content = read_file(fn)

    workflow_dict = json.loads(f_content)

    # Create the workflow JSON path and read it
    workflow_ui = dict_to_workflowui(wf=workflow_dict)

    # convert workflowui to workflow
    work_flow = workflowui_to_workflow(workflow_ui)

    '''
    with open(fn, "r") as workflow_file:
        workflow_dict = json.load(workflow_file)

        # Create the workflow JSON path and read it
        workflow_ui = dict_to_workflowui(wf=workflow_dict)

        # convert workflowui to workflow
        work_flow = workflowui_to_workflow(workflow_ui)
    '''

    wc_temp = RestWorkflowContext(postback_url, job_id)

    clusterbool: bool = False
    if cluster == "true":
        clusterbool = True

    job_context = JobContext(clusterbool, wc_temp)
    job_context.runningOn = 0
    job_context.outputs = []
    job_context.workflow = work_flow

    # Run the workflow
    run_workflow_created_from_file(job_context, workflow=work_flow)


# first parameter is the workflow json file path

# --workflow-file workflowjson/readcsv_columnfilter.json --cluster false --debug false --sql-context SQLContext --postback-url http://localhost --var n1=v1 --var n2=v2  --var n3=v3

"""Processes the incoming arguments"""


def processargs():
    print("processing args")

    numargs = len(sys.argv)
    print('Number of arguments:', numargs, 'arguments.')
    print('Argument List:', str(sys.argv))

    parser = argparse.ArgumentParser()
    parser.add_argument('-w', '--workflow-file')
    parser.add_argument('-c', '--cluster')
    parser.add_argument('-d', '--debug')
    parser.add_argument('-s', '--sql-context')
    parser.add_argument('-u', '--hive-url')
    parser.add_argument('-p', '--postback-url')
    parser.add_argument('-j', '--job-id')
    parser.add_argument('-v', '--var')

    args = parser.parse_args()

    print("FINISHED PARSING ARGS")

    return args


"""Reads in the given workflow json from a file. It then executes the workflow."""
if __name__ == '__main__':

    args = processargs()

    cluster = "true"
    if args.cluster is not None:
        cluster = args.cluster

    workflow_file = "../../workflowjson/csvs_join.json"
    if args.workflow_file is not None:
        workflow_file = args.workflow_file

    job_id = "1"
    if args.job_id is not None:
        job_id = args.job_id

    postback_url = "http://localhost:8080"
    if args.postback_url is not None:
        postback_url = args.postback_url

    try:
        run_workflow_from_file(cluster, workflow_file, postback_url, job_id)
    except Exception as e:
        errstr = "Error : " + str(e)

        workflowContext = RestWorkflowContext(postback_url, job_id)
        workflowContext.outFailureValues(1, "Failure", errstr)

        print(errstr)
        traceback.print_exc()
