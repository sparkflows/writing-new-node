# fire-examples

This repo contains the following:

* fire jar
* this README file which provides details on configuring and running the server
* example workflows and data which you can start using immediately

Below is the directory structure of the files mentioned above.

* .../fire-examples/
	* fire-core-1.2.0.jar : fire core jar which contains the fire Nodes and Workflow engine
	* data/ : sample data files
	* datasets/ : contains dataset json file capturing the schema of the data
	* workflows-spark/ : contains sample workflow json file for various workflows



## Building

### Install the Fire jar to the local maven repository

Fire examples depends on the Fire jar file. Use the below commands to install the fire jar in your local maven repo.

    mvn install:install-file -Dfile=fire-core-1.2.0.jar -DgroupId=fire  -DartifactId=fire-core  -Dversion=1.2.0 -Dpackaging=jar
    
    
    
### Build with Maven

    mvn package
    
## Developing with IntelliJ

IntelliJ can be downloaded from https://www.jetbrains.com/idea/

    Add the scala plugin into IntelliJ.
    Import fire-examples as a Maven project into IntelliJ.

## Developing with Eclipse

Scala IDE for Eclipse can be downloaded from http://scala-ide.org/

    Import fire-examples as a Maven project into Eclipse.

# Running the example workflows on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflows.

	hadoop fs -put data

Below are commands to run the various example Workflows on a Spark cluster. 

Executors with 1G and 1 vcore each have been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar cluster

	spark-submit --class fire.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar localhost:8080 --job-id 1 --workflow-file workflows-spark/kmeans.json

## Building and Deploying example Nodes

This repo has an example Node : fire.examples.node.NodeTestPrintFirstNRows

Building this repo generates:

	target/fire-examples-1.2.0.jar
	target/fire-examples-1.2.0-jar-with-dependencies.jar

The details for writing a New Node is here : https://github.com/sparkflows/fire-examples/blob/master/CreatingNewNodes.md

## Run a Java/Scala json workflow from the command line

	The workflow can be created from the Sparkflows user interface. Each workflow has a json representation.

	Below, the workflows is workflows-spark/kmeans.wf

	spark-submit --class fire.examples.workflow.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar --workflow-file workflows-spark/kmeans.wf

	
## Display the example Node in fire-ui and run it from there

	Add the fire-examples-1.2.0.jar to fire-lib directory
	Create and add the node json to nodes/examples
	Restart fire-ui
	Include fire-examples-1.2.0.jar for the workflows where it is needed

	Create an uber jar with fire-jar and your example nodes

	


