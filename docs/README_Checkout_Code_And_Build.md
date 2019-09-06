## Checkout Code, Build and Deploy New Custom Processor into Fire Insights

With the steps below, you can check our the code for a sample custom processor, build it, deploy it in Fire Insights and start using it.

### Check out the code

    git clone https://github.com/sparkflows/writing-new-node.git

### Install the Fire jar to the local maven repository

Writing new node depends on the Fire jar file. The Fire jar file provides the parent class for any new node. 

Use one of the commands below to install  fire jar in your local maven repo for Apache Spark 2.3 or Apache Spark 2.1.

    mvn install:install-file -Dfile=fire-spark_2.3-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_2.3-core  -Dversion=3.1.0 -Dpackaging=jar
    
    mvn install:install-file -Dfile=fire-spark_2_1-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_2_1-core  -Dversion=3.1.0 -Dpackaging=jar
    
### Build with Maven

    mvn package

### Jar files

Building this repo generates the following jar files:

	target/writing-new-node-3.1.0.jar
	target/writing-new-node-3.1.0-jar-with-dependencies.jar

The details for coding a New Node is here : https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Writing_New_Nodes.md

### Running the workflow locally

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

Below is the command to execute the example Workflow on an Apache Spark cluster. 

The parameter 'cluster' specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to 'local' or not specifying it.

	spark-submit --master yarn --deploy-mode client --class fire.workflows.examples.WorkflowTest target/writing-new-node-3.1.0-jar-with-dependencies.jar cluster


### Display the example Node in Fire Insights and run it from there

New nodes written can be made visible in the Fire Insights UI. Thus, the users can start using them immediately.

* Copy the **writing-new-node-3.1.0.jar** to **fire-user-lib** directory of the sparkflows install
* Copy **testprintnrows.json** to the **nodes** directory under sparkflows install
* Restart Fire Insights : ./run-fire-server.sh restart
* **TestPrintNRows** node would now be visible in the workflow editor window and you can start using it.


