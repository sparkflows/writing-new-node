## Checkout Code, Install Fire jar

With the steps below, you can check out the code for a sample custom processor, build it, deploy it in Fire Insights and start using it.

### Check out the code

    git clone https://github.com/sparkflows/writing-new-node.git

## Install the Fire jar to the local maven repository

Writing new node depends on the Fire Insights core jar file. This jar provides the parent class for any new node. 

Use the command below to install fire core jar in your local maven repo. Use the appropriate Spark version.

    cd writing-new-node
    mvn install:install-file -Dfile=fire-spark_3.0.1-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_3.0.1-core  -Dversion=3.1.0 -Dpackaging=jar
    

