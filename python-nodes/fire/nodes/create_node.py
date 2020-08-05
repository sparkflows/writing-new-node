
from fire.workflowengine import *
from fire.nodes.etl import *
from fire.nodes.dataset import *
from fire.customnode import *
#from ..create_custom_node import createCustomNode


def getInputParameters(fields):

    parameters_mapping = {}

    for i in range(len(fields)):
        parameters_mapping[fields[i]['name']] = fields[i]['value']

    return parameters_mapping


def createNode(wfui: WorkflowUI, nodeui: NodeUI):

    nodeid = int(nodeui.id)

    fields = nodeui.fields
    parameters_mapping = getInputParameters(fields)

    node = createCustomNode(
        nodeui.node_class,
        nodeid,
        nodeui.name,
        nodeui.description,
        parameters_mapping)

    if node is not None:
        return node


    if nodeui.node_class == "fire.nodes.util.NodePrintFirstNRows":
        node = NodePrintNRows(
            nodeid,
            nodeui.name,
            nodeui.description,
            parameters_mapping)

    if nodeui.node_class == "fire.nodes.dataset.NodeDatasetCSV":
        node = NodeReadCSV(
            nodeid,
            nodeui.name,
            nodeui.description,
            parameters_mapping)

    if nodeui.node_class == "fire.customnode.NodeVariableSelection":
        node = NodeVariableSelection(
            nodeid,
            nodeui.name,
            nodeui.description,
            parameters_mapping)