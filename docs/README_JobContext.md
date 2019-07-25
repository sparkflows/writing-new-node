## JobContext

The execute method of a Node gets passed the JobContext.

### JobContext variables

JobContext gives access to the following useful variables:

* HashMap<String, String> parameters

It contains a map of the parameters name and value

* Workflow workflow

It gives access to the Workflow which is being executed.

* String userName

It is the name of the user who is executing the workflow
