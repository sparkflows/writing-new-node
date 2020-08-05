import copy

from fire.workflowengine.jobcontext import *
from fire.mlmodels.fire_mlmodel import *
from numpy import ndarray

from pyspark.sql import DataFrame
import operator

class EdgeDataFrameTuple:
    def __init__(self, edge: int, dataFrame: DataFrame):
        self.edge = edge
        self.dataFrame = dataFrame


class EdgeFireSchemaTuple:
    def __init__(self, edge: int, fireSchema: FireSchema):
        self.edge = edge
        self.fireSchema = fireSchema


class Node:
    def __init__(self, wf_id: int, name: str, description: str):
        self.id = wf_id
        self.name = name
        self.description = description

        self.dataFrame = None
        self.dataFrame1 = None
        self.nextNodes = []

        self.numIncomingEdges = 0
        self.nodeExecuted = False

        self.dataFrames = []
        self.inputSchemas = []

        self.gettingOutputSchema = False

        self.mlmodel = None

        # self.ndarray = None
        # self.ndarray1 = None

    def persistOutputDataframes(self):
        return

    def getNode(self, idx: int):
        if ( len(self.nextNodes) >= (idx +1)):
            return self.nextNodes[idx];

        return None

    def addFireMLModel(self, mlmodel: FireMLModel):
        self.mlmodel = mlmodel;

    def addDataFrame(self, edge_id: int, df: DataFrame):

        self.dataFrames.append((edge_id, df))

        if self.dataFrame is None:
            self.dataFrame = df
            return
        elif self.dataFrame is not None and self.dataFrame1 is None:
            self.dataFrame1 = df
            return

    def canRunSynchronous(self):
        return True

    def execute(self, jobContext: JobContext):
        print("Executing node : " + str(id))

    def __str__(self):
        return str(self.id) + " : " + self.name + " : " + self.description

    def pass_model_to_next_nodes(self, model: FireMLModel):
        for node in self.nextNodes:
            node.mlmodel = model

    def pass_scikit_model_to_next_nodes(self, model: LinearModel):

        mlmodel = FireMLModel(ModelType.SKLEARN, sklearnModel= model, kerasModel=None)    # TTTTTTTTTTTTTTTTTTTTT
        for node in self.nextNodes:
            node.mlmodel = mlmodel

        return mlmodel

    def pass_keras_model_to_next_nodes(self, model: Sequential):
        mlmodel = FireMLModel(type=ModelType.KERAS, sklearnModel=None, kerasModel=model)
        for node in self.nextNodes:
            node.mlmodel = mlmodel
        return mlmodel

    def pass_dataframe_to_next_nodes(self, job_context: JobContext, df1: DataFrame, df2: DataFrame):
        nextNode1 = self.getNode(0)
        if (nextNode1 != None):
            edge_id = job_context.workflow.getEdgeId(self.id, nextNode1.id)
            nextNode1.addDataFrame(edge_id, df1)

        nextNode2 = self.getNode(1)
        if (nextNode2 != None):
            edge_id = job_context.workflow.getEdgeId(self.id, nextNode2.id)
            nextNode2.addDataFrame(edge_id, df2)

    # def pass_ndarray_to_next_nodes(self, job_context: JobContext, ndarray1: ndarray, ndarray2: ndarray):
    #     nextNode1 = self.getNode(0)
    #     if(nextNode1 != None):
    #         edge_id = job_context.workflow.getEdgeId(self.id, nextNode1.id)
    #         nextNode1.addDataFrame(edge_id, ndarray1)
    #
    #     nextNode2 = self.getNode(1)
    #     if (nextNode2 != None):
    #         edge_id = job_context.workflow.getEdgeId(self.id, nextNode2.id)
    #         nextNode2.addDataFrame(edge_id, ndarray2)

    def pass_dataframe_to_next_nodes_and_execute(self, job_context: JobContext, df: DataFrame):

        self.nodeExecuted = True

        # output the dataframe to fire-ui in synchronous mode
        if job_context.runningOn == RunningOn.LOCALLY_SYNCHRONOUS and job_context.executeTillNodeId > -1:
            #job_context.saveDataFrameToJobContext(df, self.id, self.name, self.name)
            job_context.workflowContext.outDataFrame(self.id, self.name, df)

        # if execution needs to stop here
        if self.id == job_context.executeTillNodeId:

            if df is None:
                job_context.stopExecution = True
                return

            fireSchema = FireSchema.createFireSchema(df.schema)
            job_context.outputSchema = fireSchema

            job_context.stopExecution = True

            return

        if job_context.runningOn == RunningOn.LOCALLY or job_context.runningOn == RunningOn.CLUSTER:
            job_context.workflowContext.outSchema(self.id, "Output Schema", df)

        # pass the dataframe to the next nodes
        for node in self.nextNodes:
            edge_id = job_context.workflow.getEdgeId(self.id, node.id)
            node.addDataFrame(edge_id, df)

        # execute the next nodes
        self.execute_next_nodes(job_context)

    def execute_next_nodes(self, job_context: JobContext):

        self.nodeExecuted = True

        for node in self.nextNodes:

            if job_context.runningOn == RunningOn.LOCALLY_SYNCHRONOUS and node.canRunSynchronous() == False:
                outputText = OutputText(
                    node.id,
                    "Error",
                    "Error",
                    "Cannot execute node in the Editor",
                    resultType=99,
                    visibility="EXPANDED")
                # job_context.workflowContext.outText(outputText)
                job_context.outputs.append(outputText)
                return

            if job_context.workflow.canExecuteNode(node) == False:
                return

            node.nodeExecuted = True

            self.dataFrames.sort(key = operator.itemgetter(0))
            node.printExecutingNodeAndInputSchema(job_context)
            node.execute(job_context)

    def getOutputSchema(
            self,
            context: InputSchemaContext,
            inputSchema: FireSchema):
        print("Executing getOutputSchema in Node : nodeId : " + self.name)
        return inputSchema

    def getExecutingString(self):
        return "Executing Node " + self.name + " : " + str(self.id)

    def printExecutingNode(self, job_context: JobContext):
        job_context.workflowContext.outHeaderValues(
            self.id, self.name, self.getExecutingString())

    def printExecutingNodeAndInputSchema(self, job_context: JobContext):
        job_context.workflowContext.outHeaderValues(
            self.id, self.name, self.getExecutingString())
        if self.dataFrame is not None:
            job_context.workflowContext.outSchema(
                self.id, "Input Schema", self.dataFrame)

    def getListOfValues(self, input_col: str):
        return input_col.strip('[').strip(']').replace('"', '').split(',')


class Nodes:
    def __init__(self, nodes: List[Node] = []):
        self.nodes = nodes


class Edge:
    def __init__(self, edge_id: int, source: int, dest: int):
        self.id = edge_id
        self.source = source
        self.dest = dest


class Edges:
    def __init__(self, edges: List[Edge] = []):
        self.edges = edges


class Workflow:
    def __init__(self, name: str, nodes: Nodes, edges: Edges):
        self.name = name
        self.nodes = nodes
        self.edges = edges
        self.gettingOutputSchema = False

    def setNumIncomingConnections(self):
        for node in self.nodes:
            node.numIncomingEdges = 0

        for edge in self.edges:
            node = self.get_node(edge.dest)
            node.numIncomingEdges = node.numIncomingEdges + 1

    def get_edge(self, num: int):
        for edge in self.edges:
            if edge.id == num:
                return edge

    def getEdgeId(self, fromNodeId: int, toNodeId: int):
        for edge in self.edges:
            if edge.source == fromNodeId and edge.dest == toNodeId:
                return edge.id

        return -1

    def get_node(self, num: int):
        for node in self.nodes:
            if node.id == num:
                return node

    def get_num_edges(self):
        count = 0
        for edge in self.edges:
            count += 1
        return count

    def getNumIncomingEdges(self, node: Node):
        numIncomingEdges = 0

        for edge in self.edges:
            if edge.dest == node.id:
                numIncomingEdges += 1

        return numIncomingEdges

    def canExecuteNode(self, node: Node):
        if node.nodeExecuted:
            return False

        dict = {}

        for n in self.nodes:
            # dict.update( { n.id, n} )
            dict[n.id] = n

        for edge in self.edges:
            if edge.dest == node.id:
                prevnode = dict.get(edge.source, None)
                if not prevnode.nodeExecuted:
                    return False

        return True

    def startingNodesToExecuteForExecuteTill(self, executeTillNodeId: int):
        startingNodes = []
        for node in self.nodes:
            if node.numIncomingEdges == 0:
                startingNodes.append(node)

        return startingNodes

    def clearNextNodes(self):
        for node in self.nodes:
            node.nextNodes = []

    def clearNodeExecuted(self):
        for node in self.nodes:
            node.nodeExecuted = False

    def setNextNodes(self):
        self.clearNextNodes()

        for edge in self.edges:
            from_node = self.get_node(edge.source)
            dest_node = self.get_node(edge.dest)
            from_node.nextNodes.append(dest_node)

    # get input schema of nodeId
    def getInputSchema(self, nodeId: int):
        self.clearNextNodes()
        self.setNextNodes()
        self.setNumIncomingConnections()
        self.clearNodeExecuted()

        startingNodes = self.startingNodesToExecuteForExecuteTill(0)

        for node in startingNodes:

            node.nodeExecuted = True
            schemaarr = self.getInputSchema_1(node, nodeId, FireSchema(), -1)
            if schemaarr is not None:
                return schemaarr

        return []

    # get the input schema of nodeId.
    # current node is node with given input schema coming from previous node
    # with fromNodeId

    def getInputSchema_1(
            self,
            node: Node,
            nodeId: int,
            inputSchemaOfThisNode: FireSchema,
            fromNodeId: int):
        print("Workflow::getInputSchema" + str(nodeId))

        if fromNodeId >= 0:
            fromNode: Node = self.get_node(fromNodeId)
            edgeId: int = self.getEdgeId(fromNode.id, node.id)
            edgeFireSchemaTuple = EdgeFireSchemaTuple(
                edgeId, inputSchemaOfThisNode)
            node.inputSchemas.append(edgeFireSchemaTuple)
        else:
            edgeFireSchemaTuple = EdgeFireSchemaTuple(0, inputSchemaOfThisNode)
            node.inputSchemas.append(edgeFireSchemaTuple)

        if node.numIncomingEdges > len(node.inputSchemas):
            print("Number of Incoming edges not met. So returning None")
            return None

        if nodeId == node.id:
            print("Got input schema for node :: " +
                  str(node.id) + " :: " + node.name)
            resultsch = []
            for s in node.inputSchemas:
                resultsch.append(s.fireSchema)
            return resultsch

        print("Passing control to next nodes as id did not match")

        for n in node.nextNodes:

            # get the output schema of this node : create a copy of this schema
            # so that it is not modified
            tempSchema = copy.deepcopy(inputSchemaOfThisNode)
            outputSchema = node.getOutputSchema(None, tempSchema)

            schemaarr = self.getInputSchema_1(n, nodeId, outputSchema, node.id)
            if schemaarr is not None:
                return schemaarr

        return None

