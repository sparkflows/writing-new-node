    
### Build with Maven

    mvn clean package

### Jar files

Building this repo generates the following jar files:

	target/writing-new-node-3.1.0.jar
	target/writing-new-node-3.1.0-jar-with-dependencies.jar

### Running the workflow locally

Use the command below to run the example workflow locally.

       java -cp target/writing-new-node-3.1.0-jar-with-dependencies.jar fire.workflows.examples.WorkflowTestConcatColumns

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

Use the command below to load example data onto HDFS. It is used by the example Workflows.

	hadoop fs -put data

Below is the command to execute the example Workflow on an Apache Spark cluster. 

The parameter 'cluster' specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to 'local' or not specifying it (default value is local).

	spark-submit --master yarn --deploy-mode client --class fire.workflows.examples.WorkflowTestConcatColumns target/writing-new-node-3.1.0-jar-with-dependencies.jar cluster


### Deploying the example Node into Fire Insights

New nodes written can be deployed into Fire Insights. Thus, the users can start using them from Fire Insights UI.

* Copy the **writing-new-node-3.1.0.jar** to **fire-user-lib** directory of the sparkflows install
* Copy **testprintnrows.json** to the **nodes** directory under sparkflows install
* Restart Fire Insights Server : **./run-fire-server.sh restart**
* **TestPrintNRows** node would now be visible in the workflow editor window and you can start using it.


