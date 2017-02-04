# writing-new-node

This repository contains a sample Node based on which new nodes can be created. This Node can be placed in the sparkflows web server and executed.

## Directory Contents

Below is the contents of the directory.

* fire-core-1.3.0.jar
    * fire core jar which contains the fire Nodes and Workflow engine
* data
    * sample data files
* src/main/java/fire/examples/nodes/NodeTestPrintFirstNRows.java
    * example node
* src/main/java/fire/examples/workflow/WorkflowTest.java
    * example workflow which uses the node NodeTestPrintFirstNRows and executes it
* pom.xml
* README.md
    * this README file which provides the steps of execution.

## Building

### Install the Fire jar to the local maven repository

Writing new Node depends on the Fire jar file. The Fire jar file provides the parent class for any new Node. Use the below commands to install the fire jar in your local maven repo.

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

# Running the workflow on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflow.

	hadoop fs -put data

Below is the command to execute the example Workflow on a Spark cluster. 

Executors with 1G and 1 vcore each have been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.WorkflowTest --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/writing-new-node-1.3.0-jar-with-dependencies.jar cluster


## Jar files

Building this repo generates the following jar files:

	target/writing-new-node-1.3.0.jar
	target/writing-new-node-1.3.0-jar-with-dependencies.jar

The details for coding a New Node is here : https://github.com/sparkflows/writing-new-node/blob/master/CreatingNewNodes.md

## Run a Java/Scala json workflow from the command line

The workflow can be created from the Sparkflows user interface. Each workflow has a json representation.

Below, the workflow is workflows-spark/kmeans.wf

	spark-submit --class fire.examples.workflow.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 1G  --num-executors 1  --executor-cores 1  target/fire-examples-1.2.0-jar-with-dependencies.jar --workflow-file workflows-spark/kmeans.wf

	
## Display the example Node in fire-ui and run it from there

New nodes written can be made visible in the Sparkflows UI. Thus, the users can start using them immediately.

* Copy the writing-new-node-1.3.0-jar-with-dependencies.jar to user-lib directory of the sparkflows install
* Create testprintnrows.json to the nodes directory under sparkflows install
* Restart fire-ui
* Include writing-new-node-1.3.0.jar for the workflows where it is needed


	


