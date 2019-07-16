## Checkout Code, Build and Start using the New Custom Processor

### Check out the code

Check out the code with : **git clone https://github.com/sparkflows/writing-new-node.git**

### Install the Fire jar to the local maven repository

Writing new Node depends on the Fire jar file. The Fire jar file provides the parent class for any new Node. Use the below commands to install the fire jar in your local maven repo.

    mvn install:install-file -Dfile=fire-spark_2_1-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_2_1-core  -Dversion=3.1.0 -Dpackaging=jar
    
### Build with Maven

    mvn package

### Jar files

Building this repo generates the following jar files:

	target/writing-new-node-3.1.0.jar
	target/writing-new-node-3.1.0-jar-with-dependencies.jar

The details for coding a New Node is here : https://github.com/sparkflows/writing-new-node/blob/master/CreatingNewNodes.md

## Running the workfow locally

Use the command below to run the example workflow locally.

       java -cp target/writing-new-node-3.1.0-jar-with-dependencies.jar fire.workflows.examples.WorkflowTest

It would finally print the following values.

       DoubleType DoubleType StringType 
       3.0 2.0 3.0|2.0 
       1.1 1.0 1.1|1.0 
       4.1 5.0 4.1|5.0 
       3.1 6.0 3.1|6.0 
       2.1 2.0 2.1|2.0 
       2.3 3.0 2.3|3.0 
       3.0 2.0 3.0|2.0 
       1.1 1.0 1.1|1.0 
       4.1 5.0 4.1|5.0 
       3.1 6.0 3.1|6.0 

### Running the workflow on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflow.

	hadoop fs -put data

Below is the command to execute the example Workflow on a Spark cluster. 

Executors with 1G and 1 vcore each have been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.workflows.examples.WorkflowTest --master yarn-client target/writing-new-node-3.1.0-jar-with-dependencies.jar cluster


### Display the example Node in fire-ui and run it from there

New nodes written can be made visible in the Sparkflows UI. Thus, the users can start using them immediately.

* Copy the **writing-new-node-3.1.0.jar** to **fire-server-lib** and **fire-user-lib** directory of the sparkflows install
* Copy **testprintnrows.json** to the **nodes** directory under sparkflows install
* Restart fire
* Restart fire-ui
* **TestPrintNRows** node would now be visible in the workflow editor window and you can start using it.


