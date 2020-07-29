# Writing new PySpark Node

`REQUIRES PYTHON  VERSION >= 3.6.0`

- [Install Fire Insights](https://www.sparkflows.io/download)

- unzip jobs.zip to get the code
```
    cd fire-3.1.0/dist
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

[Create Node json](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Processor_JSON.md) by adding the `"engine": "pyspark"` and default engine is scala/java.

```
{
  "id": "10",
  "name": "SamplePrintFirstNRows",
  "description": "Prints the specified number of records in the DataFrame. It is useful for seeing intermediate output",
  "type": "transform",
  "engine": "pyspark",
  "nodeClass": "fire.nodes.util.NodeSamplePrintFirstNRows",
  "fields" : [
  	{"name":"title", "value":"Row Values", "widget": "textfield", "title": "Title"},
    {"name":"n", "value":"10", "widget": "textfield", "title": "Num Rows to Print","description":"number of rows to be printed"}
  ]
}
```

## Local development setup:

Check the `requirements.txt` to install all the required packages.

`pip install -r requirements.txt`

#### Step 1:
            Import the fire pyspark as a project from `fire-3.1.0/dist/fire` to PyCharm(Any IDE).
            
            To make sure everything is setup, run the `run_from_file.py`, which executes the sample workflow end-to-end.    
#### Step 2:
            Create H2 db: ./create-h2-db.sh
            Start fire server: ./run-fire-server.sh start
            
            Start fire(pyspark server): python __main__
         
Follow the Writing new PySpark Node section.                
