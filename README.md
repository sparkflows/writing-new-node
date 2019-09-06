## Overview

In Fire Insights, you can write your own processor in Apache Spark and plug them into Fire Insights. Once these processors are made available to Fire Insights via a jar file and field definitions captured in a JSON file, they appear within the Fire Insights Workflow Editor. Users can use these new processors like any other processor within Fire Insights. This repository contains a few sample processors to get you started.

<!--- # For more examples on writing nodes in Sparkflows, refer: -->

<!--- https://github.com/sparkflows/sparkflows-stanfordcorenlp -->

<!--- 
 This should be the structure: 
  Step 1:  Check out from git - Just include checking out and installing jar/code
      - move content from Developing with IntelliJ/Scala IDE for Eclipse here
      - Move building and deploying to step 6
  Step 2: Copy " Directory Contents" and explain the contents
  Step 3: Explain node hierarchy including a hierarchy diagram 
  Step 4: Creating new node 
         - Creating new connectors node 
            - Creating JSON  - show real JSON code in help - add more comments to code 
            - Creating node implementation - show real JSON code in help - add more comments to code 
            - Creating node rules - show real JSON code in help - add more comments to code 
         - Creating new processor node 
             - Follow same structure as connector node
         - Creating new Machine learning node 
            - Follow same structure as connector node
 Step 5: Running Test workflow to test node 
 Step 6: Deploying your new node
--> 

## Steps

- [Checkout Code](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Checkout_Code.md)
- [Directory Contents](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Directory_Contents.md)
- [Running Workflow JSON With Spark Submit](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Running_Workflow_JSON_With_Spark_Submit.md)


## Writing new Java/Scala Nodes

- [Node Class Hierarchy](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Node_Class_Hierarchy.md)
- [Checkout Code, Build and Deploy New Custom Processor into Fire Insights](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Checkout_Code_And_Build.md)
- [Writing New Java/Scala Nodes](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Writing_New_Nodes.md)


## Reference Docs

- [Processor/Node JSON & Widgets](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Processor_JSON.md)
- [JobContext and Workflow](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_JobContext.md)
- [Writing Machine Learning Nodes](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Writing_Machine_Learning_Nodes.md)
- [Workflow Parameters & Node Variables](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Node_Variables.md)

## Writing new PySpark Nodes

- [Writing new PySpark Nodes](https://github.com/sparkflows/writing-new-node/blob/master/docs/README_Writing_New_PySpark_Node.md)

## Workflow

<img src="https://github.com/sparkflows/writing-new-node/blob/master/docs/images/workflow.png"/>

