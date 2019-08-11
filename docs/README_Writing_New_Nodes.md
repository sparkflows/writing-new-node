# Writing New Nodes

New Nodes can be easily added for adding new functionality.

## Spark DataFrames

Spark DataFrame is used to represent the distributed data in the Workflows. They are used to pass data from one node
to another.

A DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table
in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be
constructed from a wide array of sources such as: structured data files, tables in Hive, external databases,
or existing RDDs.

http://spark.apache.org/docs/latest/sql-programming-guide.html


## Create the New Node

* Create the New Node as a Java or Scala class by extending the **Node** class

## Overide the execute() method in the New Node:

    @Override
    public void execute(JobContext)

JobContext provides access to SparkContext, SQLContext etc.

## Overide the getOutputSchema() method in the new Node

If the new Node changes the incoming schema, override the getSchema() method to return the new output schema from the node.

    @Override
    public FireSchema getOutputSchema(Workflow workflow, FireSchema inputSchema) 

## Examples

The following folders contain some examples of new Nodes in this repo written in Java and in Scala:

* https://github.com/sparkflows/writing-new-node/tree/master/src/main/java/fire/nodes/examples
* https://github.com/sparkflows/writing-new-node/tree/master/src/main/scala/fire/nodes/examples

