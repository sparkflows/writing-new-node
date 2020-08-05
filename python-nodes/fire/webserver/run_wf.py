from pyspark.sql import SparkSession, DataFrame

from fire.workflowengine import *
from fire.workflowengine.configurationservice import *
from fire.nodes.dataset import *


def execute(prevnode: Node, node: Node, job_context: JobContext):
    print(node)

    df: DataFrame = None
    if prevnode is not None:
        df = prevnode.df

    node.execute(job_context, df)

    return


def run_workflow(
        jobContext: JobContext,
        configurationService: ConfigurationService,
        user_name: str,
        workflow: Workflow):

    jobContext.outSparkContext()

    workflow.setNumIncomingConnections()
    workflow.setNextNodes()

    '''
    sorted_nodes_ids = []
    for node in workflow.nodes:
        sorted_nodes_ids.append(node.id)

    print(sorted_nodes_ids)

    sorted_nodes_ids.sort()

    prevnode : Node = None
    '''
    print(configurationService.app_runOnCluster)
    if (configurationService.app_runOnCluster):
        for node in workflow.nodes:
            if (isinstance(node, NodeDataset)):
                path = SparkPathUtil.getSparkPath(
                    configurationService, node.path, user_name)
                print('path :', path)
                node.path = path

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

    '''
    for node_id in sorted_nodes_ids:
        node: Node = workflow.get_node_1(node_id)
        print("XXXXXXXXXXXXX : Executing node : " + str(node))
        execute(prevnode, node, job_context)
        prevnode = node
    '''

    '''
    # start executing from the first node
    for node_id in sorted_nodes_ids:
        # execute first node
        sources = node_state[node]['sources']
        name = node_state[node]['name']
        previous_nodes = node_state[node]['previous_nodes']
        executed = node_state[node]['executed']
        # check if all sources execute or if it is the first node
        if len(sources) == 1 and sources[0] == -1 and previous_nodes == [] and executed == 0:
            # first node - execute
            node_state = execute(node=node, job_context=job_context, node_state=node_state)

        # run other nodes
        elif executed == 0:
            for previous_node in previous_nodes:
                if node_state[previous_node]['executed'] == 0:
                    node_state = execute(name=name, data_path=data_path, job_context=job_context, node_state=node_state,
                            node_id=previous_node)

            # all previous nodes have been executed now - run the current node
            node_state = execute(name=name, data_path=data_path, job_context=job_context, node_state=node_state, node_id=node)
    return node_state
    '''

    return
