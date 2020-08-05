import argparse
import subprocess
import sys
import json

from fire.workflowengine.jobcontext import JobContext, RunningOn
from fire.workflowcontext import *
from fire.workflowengine.workflow import Node, Nodes, Edge, Edges, Workflow
from fire.nodes.dataset.node_readcsv import NodeReadCSV
from fire.nodes.etl.node_printnrows import NodePrintNRows
from fire.webserver.run_from_file import run_workflow_created_from_file


if __name__ == '__main__':
    print("Running workflow csv")

    # create ConsoleWorkflowContext
    postback_url = ""
    job_id = ""
    wc_temp = ConsoleWorkflowContext()

    clusterbool: bool = False

    # create job context
    job_context = JobContext(clusterbool, wc_temp)
    job_context.runningOn = 0
    job_context.outputs = []

    # create read csv node
    listNodes = []

    outputColNames = "[\"id\",\"price\",\"lotsize\",\"bedrooms\",\"bathrms\",\"stories\",\"driveway\",\"recroom\",\"fullbase\",\"gashw\",\"airco\",\"garagepl\",\"prefarea\"]"
    outputColTypes = "[\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\",\"STRING\"]"
    outputColFormats = "[\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\"]"
    parameters_mapping1 = {"path":"../data/housing.csv", "header":"true",
                           "separator":",", "dropMalformed":"true",
                           "outputColNames":outputColNames, "outputColTypes":outputColTypes, "outputColFormats":outputColFormats}
    nodeCSV = NodeReadCSV(1, "csv", "desc", parameters_mapping1)

    # create print n rows node
    parameters_mapping2 = {"n": "10","title":"test"}
    nodePrinNRows = NodePrintNRows(2, "printnrows", "desc", parameters_mapping2)

    # add nodes to the list
    listNodes.append(nodeCSV)
    listNodes.append(nodePrinNRows)

    # create the edges
    listEdged = []
    edge1 = Edge(1, 1, 2)
    listEdged.append(edge1)

    # create the workflow
    work_flow = Workflow("readcsv", listNodes, listEdged)
    work_flow.gettingOutputSchema = False

    # set the workflow in the jobcontext
    job_context.workflow = work_flow
    job_context.runningOn = RunningOn.LOCALLY

    # Run the workflow
    run_workflow_created_from_file(job_context, workflow=work_flow)




