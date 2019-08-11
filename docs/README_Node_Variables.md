## Node Variables

There are cases when a value from one Node needs to be passed to the Next Nodes.

Node has the ability to receive variables from the previous Nodes. It stores them in a HashMap.

    public transient HashMap<String, Object> nodeVariables = new HashMap<>();

A Node can pass (name, value) to the next nodes using the method below:

    public void passVariableToNextNodes(JobContext jobContext, String name, Object value)


## Passing a variables from one Node to all the Next Nodes

There are cases when a variable from one Node needs to be passed to all the Next Nodes.

This can be achieved by using the method below:

    public void passVariableToAllNextNodes(JobContext jobContext, String name, Object value)

The value gets stored in the same HashMap as the case above.



