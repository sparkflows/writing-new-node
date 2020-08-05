from typing import List, Dict


class DataSetDetailUI:
    def __init__(self, id: int, uuid: str, header: str, path: str):
        self.id = id
        self.uuid = uuid
        self.header = header
        self.path = path
        # todo - fix - add other params


class DataSetDetailsUI:
    def __init__(self, datasetdetails: List[DataSetDetailUI] = []):
        self.datasetdetails = datasetdetails


class NodeUI:
    def __init__(
            self,
            id: int,
            name: str,
            description: str,
            wf_type: str,
            node_class: str,
            x: str,
            y: str,
            fields: List[Dict]):

        print("FFFFFFFFFF")
        print(fields)
        print("FFFFFFFF")

        self.id = id
        self.name = name
        self.description = description
        self.wf_type = wf_type
        self.node_class = node_class
        self.x = x
        self.y = y
        self.fields = fields

    def __str__(self):
        return str(self.id) + " : " + self.name + " : " + \
            self.description + " : " + self.node_class


class NodesUI:
    def __init__(self, nodes: List[NodeUI] = []):
        self.nodes = nodes


class EdgeUI:
    def __init__(self, id: str, source: str, dest: str):
        self.id = id
        self.source = source
        self.dest = dest

    def __str__(self):
        return str(self.id) + " : " + self.source + " : " + self.dest


class EdgesUI:
    def __init__(self, edges: List[EdgeUI] = []):
        self.edges = edges


class WorkflowUI:
    def __init__(
            self,
            name: str,
            nodes: NodesUI,
            edges: EdgesUI,
            datasetdetails: DataSetDetailsUI,
            category: str = None,
            description: str = None):
        self.name = name
        self.category = category
        self.description = description
        self.nodes = nodes
        self.edges = edges
        self.datasetdetails = datasetdetails

    def get_edge(self, num: int):
        for edge in self.edges.edges:
            if edge.id == num:
                return edge

    def get_node(self, num: str):
        for node in self.nodes.nodes:
            if node.id == num:
                return node

    def get_num_edges(self):
        count = 0
        for edge in self.edges.edges:
            count += 1
        return count
