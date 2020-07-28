# Processor/Node JSON & Widgets

The Fire Insights UI allows specifying how the dialog box of any Processor would look like. Each Processor has a corresponding json file.

Below is the example of **'NodeConcatColumns'**. A Processor can have various fields. How a field is represented in the UI is determined by the **widget** type.
In the example, we see the following **widgets** used:

- **variables** : Allows the user to select one or more variables from the incoming list of variables into the Node.
- **textfield** : Allows the user to enter some text.


```
{
  "id": "1",
  "name": "NodeConcatColumns",
  "description": "This node creates a new DataFrame by concatenating the specified columns of the input DataFrame",
  "input" : "It accepts a DataFrame as input from the previous Node",
  "output": "A new column is added to the incoming DataFrame by concatenating the specified columns. The new DataFrame is sent to the output of this Node.",
  "type": "transform",
  "nodeClass": "fire.nodes.etl.NodeConcatColumns",
  "fields" : [
    {"name":"inputCols", "value": "[]", "widget": "variables", "title": "Columns", "description": "Columns to be concatenated"},
    {"name":"outputCol", "value": "", "widget": "textfield", "title": "Concatenated Column Name", "description": "Column name for the concatenated columns"},
    {"name":"sep", "value": "|", "widget": "textfield", "title": "Separator", "description": "Separator to be used when concatenating the columns"}
  ]
}

```

## Widget Types

| Widget        | Details | Example  |
| ------------- |:-------------| -----|
| array      | Allows the user to select a value from the pre-defined values using a dropdown | {"name": "header", "value":"false", "widget": "array", "title": "Header", "optionsArray": ["true","false"], "description": "Does the file have a header row", "datatypes":["boolean"]}  |
| array_multiple      | Allows the user to select multiple values from a predefined set | {"name": "header", "value":"false", "widget": "array_multiple", "title": "Header", "optionsArray": ["year","second","season"], "description": "Time Functions to be applied"}  |
| horizontal_line  | Creates a new horizontal line in the dialog box.  | {"name": "hl", "value":"", "widget": "horizontal_line", "title": "hl"} |
| key_array  | Allows the user to enter key value pairs  | {"name": "configs", "value":"[]", "widget": "key_array", "title": "Elastic Search Configs Key"} |    
| value_array  | Allows the user to enter key value pairs  | {"name": "configs", "value":"[]", "widget": "value_array", "title": "Elastic Search Configs Value"} |  
| password     | Allows the user to enter text into a password field | {"name":"s3Password", "value":"", "widget": "password", "title": "Password", "description": "Password for S3"}  |
| schema_col_names  | Display the output schema name of the Node in a column  | {"name":"outputColNames", "value":"[]", "widget": "schema_col_names", "title": "Output Column Names", "description": "Name of the Output Columns"} |
| schema_col_types  | Display the output schema type of the Node in a column  | {"name":"outputColTypes", "value":"[]", "widget": "schema_col_types", "title": "Output Column Types", "description": "Data Type of the Output Columns"} |
| schema_col_formats  | Display the output schema format of the Node in a column  | {"name":"outputColFormats", "value":"[]", "widget": "schema_col_formats", "title": "Output Column Formats", "description": "Format of the Output Columns"} |
| tab  | Creates a new tab in the dialog box. All items below it go into the new tab  | {"name": "gridSearch", "value":"", "widget": "tab", "title": "Grid Search"} |
| textfield     | Allows the user to enter text into a textfield | {"name":"path", "value":"", "widget": "textfield", "title": "Path", "description": "Path of the Text file/directory"}  |
| variable  | Allows the user to select one of the columns from the incoming schema using a dropdown  | {"name": "featuresCol", "value":"", "widget": "variable", "title": "Features Column", "description": "Features column of type vectorUDT for model fitting", "datatypes":["vectorudt"]} |
| variables  | Allows the user to select one or more of the columns from the incoming schema  | {"name":"inputCols", "value":"[]", "widget": "variables", "title": "Input Columns", "description": "Input column of type - all numeric, boolean and vector", "datatypes":["integer", "long", "double", "float", "vectorudt"]} |
| variables_map  | Display all the incoming variables in separate rows  | {"name": "inputCols", "value":"[]", "widget": "variables_map", "title": "Variable"} |
| variables_map_edit  | Display a textfield next to the variable for the user to enter text values  | {"name": "hbaseColFamily", "value":"[]", "widget": "variables_map_edit", "title": "HBase Column Family", "description": "HBase Column Family for the variable"} |
| variables_map_select  | Displays a dropdown for the user to select from the list of available variables  | {"name": "rhsCols", "value":"[]", "widget": "variables_map_select", "title": "RHS Variables", "description": "RHS columns for matching"} |
| variables_map_array  | Allows the user to select a value from a list of available values, which are displayed in a dropdown  | {"name": "matchingAlgorithms", "value":"[]", "widget": "variables_map_array", "optionsArray": ["fullmatch", "levenshtein", "jarowinkler", "jaccard(3 gram)", "longestCommonSubsequence","notionalDistance","dateDifference"], "title": "Algorithm to use", "description": "Algorithm to use for matching" |
| variables_list_select  | Allows adding rows of values. Displays a dropdown of columns from which the user can select  | {"name": "inputCols", "value":"[]", "widget": "variables_list_select", "title": "Columns", "description": "Columns"} |
| variables_list_array  | Display a dropdown of values  |  |
| variables_list_textfield  | Allows the user to enter an editable value  | {"name": "values", "value":"[]", "widget": "variables_list_textfield", "title": "Values", "description": "Values"} |
| list_textfield  | Allows adding rows of values. Allows the user to enter an editable value for a list of fields  | {"name": "values", "value":"[]", "widget": "list_textfield", "title": "Values", "description": "Values"} |
| list_array  | Displays a dropdown of columns from which the user can select  | {"name": "values", "value":"[]", "widget": "list_array", "title": "Values", "description": "Values"} |
| array_of_values | Allow user to add/remove dynamic input textfield on button click | {"name":"tempTables", "value":"[]", "widget": "array_of_values", "title": "Temp Table Names", "description": "Temp Table Name to be used"} |
| boolean | Allows user to select option true or false | {"name":"overwrite", "value":"false", "widget": "boolean","title": "Overwrite Output"} |
| enum | Allows the user to select a value from the pre-defined map values (key:value) using a dropdown | {"name":"graphType", "value":"1", "widget": "enum", "title": "Chart Type", "optionsMap":{"LINECHART":"Line Chart","COLUMNCHART":"Side by Side Bar Chart", "BARCHART":"Stacked Bar Chart", "PIE":"Pie Chart", "SCATTERCHART": "Scatter Chart"}} |
| object_array | Allows the user to select a object using a dropdown | {"name":"connection", "value":"", "widget": "object_array", "title": "Connection", "description": "The JDBC connection to connect" ,"required":"true"} |
| variables_common | Allows the user to select one or more of the columns from the incoming schema | {"name":"joinCols", "value":"", "widget": "variables_common", "title": "Common Join Columns", "description": "Space separated list of columns on which to join"} |
| datefield | Display calendar to select date | {"name":"toDateCol", "value":"2100-12-31", "widget": "datefield", "title": "To Date", "datatypes":["date"], "description": "Takes End Date in the form of yyyy-MM-dd"} |
| key_array_join | Select left table column from dropdown to join | {"name": "leftTableJoinColumn", "value":"[]", "widget": "key_array_join",  "title": "LeftTableJoinColumn", "description": ""} |
| value_array_join | Select right table column from dropdown to join | {"name": "rightTableJoinColumn", "value":"[]", "widget": "value_array_join", "title": "RightTableJoinColumn", "description": ""} |
| sort_columns | Sort table columns | {"name": "sortColumnNames", "value":"[]", "widget": "sort_columns", "title": "Columns", "description": "Sort the Column Name"} |
| textarea_large | Display query editor | {"name":"sql", "value":"", "widget": "textarea_large", "type": "sql", "title": "SQL", "description": "SQL to be run"} |

## Examples

### ReadCSV

```
{
  "id": "11",
  "name": "ReadCSV",

  "description": "It reads in CSV files and creates a DataFrame from it",
  "input": "It reads in CSV text files",
  "output": "It creates a DataFrame from the data read and sends it to its output",

  "type": "dataset",
  "engine": "all",
  "nodeClass": "fire.nodes.dataset.NodeDatasetCSV",
  "fields" : [
    {"name":"path", "display":true,"value":"", "widget": "textfield", "required":true, "title": "Path", "description": "Path of the Text file/directory"},
    {"name":"separator", "value":",", "widget": "textfield", "title": "Separator", "description": "CSV Separator"},
    {"name": "header", "value":"false", "widget": "array", "title": "Header", "optionsArray": ["true","false"],
            "description": "Does the file have a header row"},
    {"name": "dropMalformed", "value":"false", "widget": "array", "title": "Drop Malformed", "optionsArray": ["true","false"],
      "description": "Whether to drop Malformed records or error"},

    {"name":"outputColNames", "value":"[]", "widget": "schema_col_names", "title": "Column Names for the CSV", "description": "New Output Columns of the SQL"},
    {"name":"outputColTypes", "value":"[]", "widget": "schema_col_types", "title": "Column Types for the CSV", "description": "Data Type of the Output Columns"},
    {"name":"outputColFormats", "value":"[]", "widget": "schema_col_formats", "title": "Column Formats for the CSV", "description": "Format of the Output Columns"}

  ]
}

```

### Column Filter

```
{
  "id": "11",
  "name": "ColumnFilter",
  "description": "This node creates a new DataFrame that contains only the selected columns",
  "input": "This type of node takes in a DataFrame and transforms it to another DataFrame.",
  "output": "This node filters the specified columns from the incoming DataFrame",
  "type": "transform",
  "engine": "all",
  "nodeClass": "fire.nodes.etl.NodeColumnFilter",
  "fields" : [
    {"name":"outputCols", "value": "[]", "widget": "variables", "title": "Columns", "description": "Columns to be included in the output DataFrame"}
  ]
}
```

### Math Expression

```
{
  "id": "7",
  "name": "Math Expression",
  "description": "",
  "type": "transform",
  "nodeClass": "fire.nodes.etl.NodeMathExpression",
  "fields" : [
  
   {"name": "outputCols", "value":"[]", "widget": "key_array",
      "title": "OutPut Column", "description": "Output Column Name"},

    {"name": "expressions", "value":"[]", "widget": "value_array",
      "title": "Math Expression", "description": "Define math expression."}

  ]
}
```

## Ability to Browse the HDFS/AWS S3

There are cases when we need the ability to Browse the HDFS/S3. For example when the user has to select a file or directory on HDFS/S3. This is enabled by the below example:

    {"name":"path", "value":"", "widget": "textfield", "title": "Path", "description": "Path of the Text file/directory"}

In the above having **'title'** of **'Path'** displays the Browse HDFS/S3 button using which the user is presented with a Dialog Box to browse the data in HDFS/S3.

## Allow user to write sql query

There are some scenario where user need to query sql data. In this case user can write sql query in query editor.

```
{
  "id": "11",
  "name": "SQL",
  "description": "This node runs the given SQL on the incoming DataFrame",
  "input": "This type of node takes in a DataFrame and transforms it to another DataFrame",
  "output": "This node runs the given SQL on the incoming DataFrame to generate the output DataFrame",
  "hint": "Whenever the table is changed, go to Schema tab and Refresh the Schema",
  "type": "transform",
  "engine": "all",
  "nodeClass": "fire.nodes.etl.NodeSQL",
  "fields" : [
    {"name":"tempTable", "value":"fire_temp_table", "widget": "textfield", "title": "Temp Table", "description": "Temp Table Name to be used"},
    {"name":"sql", "value":"", "widget": "textarea_large", "type": "sql", "title": "SQL", "description": "SQL to be run"},

    {"name": "schema", "value":"", "widget": "tab", "title": "Schema"},

    {"name":"outputColNames", "value":"[]", "widget": "schema_col_names", "title": "Output Column Names", "description": "Name of the Output Columns"},
    {"name":"outputColTypes", "value":"[]", "widget": "schema_col_types", "title": "Output Column Types", "description": "Data Type of the Output Columns"},
    {"name":"outputColFormats", "value":"[]", "widget": "schema_col_formats", "title": "Output Column Formats", "description": "Format of the Output Columns"}
  ]
}
```




