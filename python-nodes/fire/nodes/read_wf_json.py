from fire.nodes.dataset import *
from fire.workflowengine import *
from .create_node import createNode


def dict_to_workflowui(wf: dict):
    # with open(json_path, "r") as workflow_file:
    # wf = json.load(workflow_file)
    nodes_object = []
    for node in wf.get("nodes"):
        new_node = NodeUI(
            id=node.get("id"),
            name=node.get("name"),
            description=node.get("description"),
            wf_type=node.get("type"),
            node_class=node.get("nodeClass"),
            x=node.get("x"),
            y=node.get("y"),
            fields=node.get("fields"))
        nodes_object.append(new_node)

    edges_object = []
    for edge in wf.get("edges"):
        new_edge = EdgeUI(
            id=edge.get("id"),
            source=edge.get("source"),
            dest=edge.get("target"))
        edges_object.append(new_edge)

    dataset_details = []
    for detail in wf.get("dataSetDetails"):
        new_detail = DataSetDetailUI(
            id=detail.get("id"),
            uuid=detail.get("uuid"),
            header=detail.get("header"),
            path=detail.get("path"))
        dataset_details.append(new_detail)

    workflow = WorkflowUI(
        name=wf.get('name'),
        nodes=NodesUI(nodes_object),
        edges=EdgesUI(edges_object),
        datasetdetails=DataSetDetailsUI(dataset_details),
        category=wf.get("category"),
        description=wf.get("description"))

    return workflow


def workflowui_to_workflow(wfui: WorkflowUI):

    nodes_object = []
    for node in wfui.nodes.nodes:

        #new_node = Node(node.id, node.name, node.description)
        new_node = createNode(wfui, node)
        nodes_object.append(new_node)

    edges_object = []
    for edge in wfui.edges.edges:

        print(edge)
        new_edge = Edge(edge.id, int(edge.source), int(edge.dest))
        edges_object.append(new_edge)

    workflow = Workflow("aaa", nodes_object, edges_object)

    return workflow
