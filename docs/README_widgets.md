# Widgets

The Sparkflows UI allows specifying how the dialog box of any Processor would look like. Each Processor has a corresponding json file.

Below is the example of **'NodeDataSetCSV'**. A Processor can have various fields. How a field is represented in the UI is determined by the **widget** type.
In the example, we see the following **widgets** used:

- **textfield** : Allows the user to enter any text
- **array** : Allows the user to select a value from a given list
- Below fields are used for capturing the output schema of the Processor
    - **schema_col_names**
    - **schema_col_types**
    - **schema_col_formats**


```
{
  "id": "11",
  "name": "CSV",
  "description": "Dataset Node for reading CSV files",
  "type": "dataset",
  "nodeClass": "fire.nodes.dataset.NodeDatasetCSV",
  "fields" : [
    {"name":"path", "value":"", "widget": "textfield", "title": "Path", "description": "Path of the Text file/directory"},
    {"name":"separator", "value":",", "widget": "textfield", "title": "Separator", "description": "CSV Separator"},
    {"name": "header", "value":"false", "widget": "array", "title": "Header", "optionsArray": ["true","false"],
            "description": "Does the file have a header row", "datatypes":["boolean"]},

    {"name":"outputColNames", "value":"[]", "widget": "schema_col_names", "title": "Column Names for the CSV", "description": "New Output Columns of the SQL"},
    {"name":"outputColTypes", "value":"[]", "widget": "schema_col_types", "title": "Column Types for the CSV", "description": "Data Type of the Output Columns"},
    {"name":"outputColFormats", "value":"[]", "widget": "schema_col_formats", "title": "Column Formats for the CSV", "description": "Format of the Output Columns"}

  ]
}
```

## Widget Types

| Widget        | Details | Example  |
| ------------- |:-------------| -----|
| textfield     | Allows the user to enter text into a textfield | {"name":"path", "value":"", "widget": "textfield", "title": "Path", "description": "Path of the Text file/directory"}  |
| array      | Allows the user to select a value from the pre-defined values using a dropdown | {"name": "header", "value":"false", "widget": "array", "title": "Header", "optionsArray": ["true","false"], "description": "Does the file have a header row", "datatypes":["boolean"]}  |
| checkboxes      | Allows the user to select multiple values from a predefined set | {"name": "header", "value":"false", "widget": "array_multiple", "title": "Header", "optionsArray": ["year","second","season"], "description": "Time Functions to be applied", "datatypes":["timestamp"]}  |
| variable  | Allows the user to select one of the columns from the incoming schema using a dropdown  | {"name": "featuresCol", "value":"", "widget": "variable", "title": "Features Column", "description": "Features column of type vectorUDT for model fitting", "datatypes":["vectorudt"]} |
| variables  | Allows the user to select one or more of the columns from the incoming schema  | {"name":"inputCols", "value":"[]", "widget": "variables", "title": "Input Columns", "description": "Input column of type - all numeric, boolean and vector", "datatypes":["integer", "long", "double", "float", "vectorudt"]} |
| variables_map  | Display all the incoming variables in rows  | {"name": "inputCols", "value":"[]", "widget": "variables_map", "title": "Variable"} |
| variables_map_edit  | Display a textfield next to the variable for the user to enter text values  | {"name": "hbaseColFamily", "value":"[]", "widget": "variables_map_edit", "title": "HBase Column Family", "description": "HBase Column Family for the variable"} |
| tab  | Creates a new tab in the dialog box. All items below it go into the new tab  | {"name": "gridSearch", "value":"", "widget": "tab", "title": "Grid Search"} |


## Ability to Browse the HDFS

There are cases when we need the ability to Browse the HDFS. For example when the user has to select a file or directory on HDFS. This is enabled by the below example:

    {"name":"path", "value":"", "widget": "textfield", "title": "Path", "description": "Path of the Text file/directory"}

In the above having **'title'** of **'Path'** displays the Browse HDFS button using which the user is presented with a Dialog Box to browse the data in HDFS.


## Refreshing a field powered by custom code in a Processor

In some cases we need a field to be refreshed by custom code provided in a Processor. An example of it can be:

- Selecting a database name from a given list. In this case the custom code in the Processor would be able to fetch the list of databases.
- Selecting a table name from a given list.

Below is the example of how the code in the Node/Processor look like. It returns an array of Strings which get displayed in the dialog box as a drop down.

    @Override
    public ArrayList<String> getValue1d(String valueOf) {

        if (valueOf.toLowerCase().equals("dbtable")) {
            ArrayList<String> result = new ArrayList<>();
            result.add("sample_07");
            result.add("sample_08");

            return result;
        }

        return new ArrayList<String>();
    }
    

The Processor JSON would look like below. For dbtable, we see the widget 'array_refresh' being used.

```
{
  "id": "11",
  "name": "JDBC",
  "description": "This node reads data from other databases using JDBC.",
  "type": "dataset",
  "nodeClass": "fire.nodes.dataset.NodeDatasetJDBC",
  "fields" : [
    {"name":"url", "value":"jdbc:postgresql:dbserver", "widget": "textfield", "title": "URL", "description": "The JDBC URL to connect to"},

    {"name":"dbtable", "value":"", "widget": "array_refresh", "title": "DB Table",
      "description": "The JDBC table that should be read. Note that anything that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could also use a subquery in parentheses."},

    {"name":"driver", "value":"", "widget": "textfield", "title": "Driver",
            "description": "The class name of the JDBC driver needed to connect to this URL"},

    {"name":"outputColNames", "value":"[]", "widget": "schema_col_names", "title": "Column Names of the Table", "description": "Output Columns Names of the Table"},
    {"name":"outputColTypes", "value":"[]", "widget": "schema_col_types", "title": "Column Types of the Table", "description": "Output Column Types of the Table"},
    {"name":"outputColFormats", "value":"[]", "widget": "schema_col_formats", "title": "Column Formats", "description": "Output Column Formats"}
  ]
}
```



