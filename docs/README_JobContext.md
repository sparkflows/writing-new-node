## Overview

The execute method of a Node gets passed the JobContext object.

### JobContext

JobContext object gives access to the following useful variables in the class JobContext:

* HashMap<String, String> allVariables;

It contains a map of the variables name and value. Variables could have come in in various ways:

* Passed on the command line with --var name=value
* Added by another Processor of the workflow during execution
* Through the NodeVariables in the workflow


* Workflow workflow

It gives access to the Workflow which is being executed.

* String userName

It is the name of the user who is executing the workflow
