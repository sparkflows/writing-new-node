# fire-deployment

This repo contains the following:

* fire jar
* fire-server jar
* fire-ui jar
* properties file for configuring
* run script to start the server
* this README file which provides details on configuring and running the server
* example workflows and data which you can start using immediately

# Running Fire UI from the command line

* Create a directory called fire-ui : **mkdir fire-ui**

* Copy the fire-ui jar file into it : **fire-ui-1.0.0-SNAPSHOT.jar**
* Copy the fire-server jar file into it : **fire-server-1.0-SNAPSHOT.jar**

* Copy the nodes directory into it
* Copy the data directory into it

* Create a application.properties file in it with the following content:

	* nodesDir=nodes
	* clusterMode=false
	* fireJar=/...../fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar
	* server=localhost
	* host=localhost
	* port=9999

* Start the **fire-server** with the command below
	* **java -cp target/fire-server-1.0-SNAPSHOT.jar org.fireprojects.server.FireServer 4444 ./fire-server-logs**

* Start the **fire-ui** with the command below
	* **java -server -Xmx1548m -Xms1356m -XX:+CMSClassUnloadingEnabled -XX:PermSize=512m -XX:MaxPermSize=512m -jar fire-ui-1.0.0-SNAPSHOT.jar** 
	* **1548m** means we are giving 1.5G to the process. Increase it based on the memory on your machine.

* Go to **machine-url:8080** on your browser

Below is the directory structure of the files mentioned above.

* .../fire-ui/
	* fire-ui-1.0.0-SNAPSHOT.jar
	* fire-server-1.0-SNAPSHOT.jar
	* application.properties
	* nodes/
	* data/



## Building

### Install the Fire jar to the local maven repository

Fire examples depends on the Fire jar file. Use the below commands to install the fire jar in your local maven repo.

    mvn install:install-file -Dfile=fire-core-1.2.0-SNAPSHOT.jar -DgroupId=fire  -DartifactId=fire-core  -Dversion=1.2.0-SNAPSHOT -Dpackaging=jar
    
    
    
### Build with Maven

    mvn package -DskipTests
    
## Developing with IntelliJ

    Import fire-examples as a Maven project into IntelliJ.

## Developing with Eclipse

    Import fire-examples as a Maven project into Eclipse.
    


# Running the example workflows on a Spark Cluster

Use the command below to load example data onto HDFS. It is then used by the example Workflows.

	hadoop fs -put data

Below are commands to run the various example Workflows on a Spark cluster. 

4 executors with 2G and 3 vcores each have been specified in the commands. The parameter **'cluster'** specifies that we are running the workflow on a cluster as against locally. This greatly simplifies the development and debugging within the IDE by setting its value to **'local'** or not specifying it.

	spark-submit --class fire.examples.workflow.ml.WorkflowKMeans --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLinearRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowLogisticRegression --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.ml.WorkflowParquet --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster

	spark-submit --class fire.examples.workflow.execute.WorkflowExecuteFromFile --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster localhost:8080 1 data/workflows/kmeans.wf

	spark-submit --class fire.examples.workflow.execute.WorkflowExecuteFromJson --master yarn-client --executor-memory 2G  --num-executors 4  --executor-cores 3  examples/target/fire-examples-1.2.0-SNAPSHOT-jar-with-dependencies.jar cluster localhost:8080 1 <wf_json>





