## Overview

In Fire Insights, you can write your own processors in Apache Spark and plug them into Fire Insights.

The new Processors can either be written in Java/Scala or in Python.

Once these processors are made available to Fire Insights via a jar or python file and field definitions captured in a JSON file, they appear within the Fire Insights Workflow Editor. 

Users can use these new processors like any other processor within Fire Insights. This repository contains a few sample processors to get you started.

<!--- # For more examples on writing nodes in Sparkflows, refer: -->

<!--- https://github.com/sparkflows/sparkflows-stanfordcorenlp -->


## Steps

- [Checkout Code](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Checkout_Code.md)
- [Directory Contents](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Directory_Contents.md)
- [Build and Execute Examples](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Build_Execute_Examples.md)
- [Executing Workflow JSON With Spark Submit](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Running_Workflow_JSON_With_Spark_Submit.md)


## Reference Docs

- [Processor/Node JSON & Widgets](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Processor_JSON.md)
- [JobContext and Workflow](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_JobContext.md)
- [Workflow Parameters & Node Variables](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Node_Variables.md)

## Writing new PySpark Nodes

- [Writing new PySpark Nodes](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Writing_New_PySpark_Node.md)

## Workflow

<img src="https://github.com/sparkflows/writing-new-node/blob/master/docs/images/workflow.png"/>

