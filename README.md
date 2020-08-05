## Overview

In Fire Insights, you can write your own processors in Apache Spark and plug them into Fire Insights.

The new Processors can either be written in Java/Scala or in Python.

Once these processors are made available to Fire Insights via a jar or python file and field definitions captured in a JSON file, they appear within the Fire Insights Workflow Editor. 

Users can use these new processors like any other processor within Fire Insights. This repository contains a few sample processors to get you started.

<!--- # For more examples on writing nodes in Sparkflows, refer: -->

<!--- https://github.com/sparkflows/sparkflows-stanfordcorenlp -->

- [Writing New Java/Scala Nodes](./java_nodes/README.md)
- [Writing New Python Nodes](./python_nodes/README.md)


## Reference Docs

- [Processor/Node JSON & Widgets](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Processor_JSON.md)
- [JobContext and Workflow](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_JobContext.md)
- [Workflow Parameters & Node Variables](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Node_Variables.md)


## Workflow

<img src="https://github.com/sparkflows/writing-new-node/blob/master/docs/images/workflow.png"/>

