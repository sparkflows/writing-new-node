## Run a Java/Scala json workflow from the command line

The workflow can be created from the Sparkflows user interface. Each workflow has a json representation.

Below, the workflow is ExampleWorkflow.json

	spark-submit --class fire.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/writing-new-node-3.1.0-jar-with-dependencies.jar --workflow-file ExampleWorkflow.json

ExampleWorkflow.json consists of 3 nodes:

* fire.nodes.dataset.NodeDatasetTextFiles : Reads in the file data/cars.csv as a text file
* fire.nodes.etl.NodeFieldSplitter : Splits each line into columns c1,c2,c3,c4 using the comma as separator
* fire.nodes.examples.NodeTestPrintFirstNRows : Prints the first 10 records of the dataset

