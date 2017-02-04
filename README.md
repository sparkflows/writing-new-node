# writing-new-node

This repository contains a sample node based on which new nodes can be created.

## Contents

This repo contains the following:

* fire core jar file
* this README file which provides details on configuring and running the server
* an example node which can be run in Sparkflows

## Directory Contents

Below is the contents of the directory.

* fire-core-1.3.0.jar : fire core jar which contains the fire Nodes and Workflow engine
* data/ : sample data files
* src/main/java/fire/examples/nodes/NodeTestPrintFirstNRows.java
* src/main/java/fire/examples/workflow/WorkflowTest.java
* pom.xml
* README.md

## Building

### Install the Fire jar to the local maven repository

Writing new Node depends on the Fire jar file. Use the below commands to install the fire jar in your local maven repo.

    mvn install:install-file -Dfile=fire-core-1.3.0.jar -DgroupId=fire  -DartifactId=fire-core  -Dversion=1.3.0 -Dpackaging=jar
    
### Build with Maven

    mvn package
    
## Developing with IntelliJ

IntelliJ can be downloaded from https://www.jetbrains.com/idea/

    Add the scala plugin into IntelliJ.
    Import writing-new-node as a Maven project into IntelliJ.

## Developing with Eclipse

Scala IDE for Eclipse can be downloaded from http://scala-ide.org/

    Import fire-examples as a Maven project into Eclipse.

# Running the example workflows on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflows.

	hadoop fs -put data

Below is the command to execute the example Workflow on a Spark cluster. 

Executors with 1G and 1 vcore each have been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar cluster


## Building and Deploying example Node

This repo has an example Node : fire.examples.node.NodeTestPrintFirstNRows

Building this repo generates:

	target/writing-new-node-1.3.0.jar
	target/writing-new-node-1.3.0-jar-with-dependencies.jar

The details for writing a New Node is here : https://github.com/sparkflows/writing-new-node/blob/master/CreatingNewNodes.md

## Run a Java/Scala json workflow from the command line

The workflow can be created from the Sparkflows user interface. Each workflow has a json representation.

Below, the workflow is workflows-spark/kmeans.wf

	spark-submit --class fire.examples.workflow.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar --workflow-file workflows-spark/kmeans.wf

	
## Display the example Node in fire-ui and run it from there

New nodes written can be made visible in the Sparkflows UI. Thus, the users can start using them immediately.

	* Add the writing-new-node-1.3.0.jar to user-lib directory
	* Create and add the node json to nodes/examples
	* Restart fire-ui
	* Include fire-examples-1.2.0.jar for the workflows where it is needed
	* Create an uber jar with fire-jar and your example nodes

	


