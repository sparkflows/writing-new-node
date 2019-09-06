## Checkout Code, 

With the steps below, you can check our the code for a sample custom processor, build it, deploy it in Fire Insights and start using it.

### Check out the code

    git clone https://github.com/sparkflows/writing-new-node.git

## Install the Fire jar to the local maven repository

Writing new node depends on the Fire jar file. The Fire jar file provides the parent class for any new node. 

Use one of the commands below to install  fire jar in your local maven repo for Apache Spark 2.3 or Apache Spark 2.1.

    mvn install:install-file -Dfile=fire-spark_2.3-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_2.3-core  -Dversion=3.1.0 -Dpackaging=jar
    
    mvn install:install-file -Dfile=fire-spark_2_1-core-3.1.0.jar -DgroupId=fire  -DartifactId=fire-spark_2_1-core  -Dversion=3.1.0 -Dpackaging=jar
    
## Development

You can use IntelliJ or Scala IDE for Eclipse for your development. Feel free to use any other tool of your choice.

## Developing with IntelliJ

IntelliJ can be downloaded from https://www.jetbrains.com/idea/

    Add the scala plugin into IntelliJ.
    Import writing-new-node as a Maven project into IntelliJ.

## Developing with Scala IDE for Eclipse

Scala IDE for Eclipse can be downloaded from http://scala-ide.org/

    Import fire-examples as a Maven project into Eclipse.

