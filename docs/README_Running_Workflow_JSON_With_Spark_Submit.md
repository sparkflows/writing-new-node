## Run a Java/Scala json workflow from the command line

Workflows can also be executed on the cluster directory with the ``spark-submit`` command. The workflow can be created from the Fire Insights user interface. Each workflow is represented as a json.

In the spark-submit example below, the workflow json is ``ExampleWorkflow.json``.

	spark-submit --class fire.execute.WorkflowExecuteFromFile --master yarn-client target/writing-new-node-3.1.0-jar-with-dependencies.jar --workflow-file ExampleWorkflow.json

ExampleWorkflow.json consists of 3 processors:

* **fire.nodes.dataset.NodeDatasetTextFiles** : Reads in the file data/cars.csv as a text file
* **fire.nodes.etl.NodeFieldSplitter** : Splits each line into columns c1,c2,c3,c4 using the comma as separator
* **fire.nodes.examples.NodeTestPrintFirstNRows** : Prints the first 10 records of the dataset

