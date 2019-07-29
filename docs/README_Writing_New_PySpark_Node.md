# Writing new PySpark Node

- Install Sparkflows

- unzip jobs.zip to get the code
```
    cd .../fire-3.1.0/dist
    unzip jobs.zip
```    

- Create a new Class in python.
- Extend it from the class **Node** or **NodeDataset**. When writing a Dataset node extend NodeDataset.

- Write the __init__ method in it.
```
    def __init__(self, id: int, name: str, description: str, parameters_mapping: dict):
    (parameters_mapping provides the incoming parameters for the new node)
```
- Write the execute method in the new class
```
    def execute(self, job_context: JobContext):
        self.execute_next_nodes(job_context, self.dataFrame)
        
    (It gets the JobContext as its argument)
    (The variable dataFrame which is of type DataFrame has been populated by the incoming DataFrame into the node)
    (The execute method would pass the new DataFrame created by the method to the next nodes)
    
```

- Place the python file for the new node under **fire/customnodes** or a new directory under fire
- Update **fire/create_custom_node.py** to create the new Node of that class. Example below:
```
    if node_class == "fire.nodes.util.NodeSamplePrintFirstNRows":
        node = NodeSamplePrintNRows(nodeid, name, description, parameters_mapping)
```
- Recreate new jobs.zip which includes the new Node
```
    zip -r jobs.zip *
    chmod +x jobs.zip
```
- Restart the fire server
```
    ./run-fire-server.sh restart
```
    
## NodeSamplePrintNRows

```
class NodeSamplePrintNRows(Node):
    def __init__(self, id: int, name: str, description: str, parameters_mapping: dict):
        self.num_rows = parameters_mapping['n']
        self.title = parameters_mapping['title']

        super().__init__(id, name, description)

    def execute(self, job_context: JobContext):
        print("Executing SamplePrintNRows node : " + str(id))
        # self.dataFrame.show(5)

        if job_context.runningOn == RunningOn.LOCALLY_SYNCHRONOUS and job_context.executeTillNodeId > -1:
            print("")
        else:
            job_context.workflowContext.outDataFrame(self.id, self.title, self.dataFrame)

        self.execute_next_nodes(job_context, self.dataFrame)

```


