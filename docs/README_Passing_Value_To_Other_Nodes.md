## Passing any value from one Node to the Next Nodes

There are cases when a value from one Node needs to be passed to the Next Nodes.

Node has the ability to receive objects from the previous Nodes. It stores them in a HashMap.

    public transient HashMap<String, Object> incomingValues = new HashMap<>();

A Node can pass (name, value) to the next nodes using the method below:

    public void passValueToNextNodes(JobContext jobContext, String name, Object value)


## Passing any value from one Node to all the Next Nodes

There are cases when a value from one Node needs to be passed to all the Next Nodes.

This can be achieved by using the method below:

    public void passValueToAllNextNodes(JobContext jobContext, String name, Object value)

The value gets stored in the same HashMap as the case above.



