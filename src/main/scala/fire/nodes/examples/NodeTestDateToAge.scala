package fire.nodes.examples

import fire.context.JobContext
import fire.workflowengine.{FireSchema, Workflow, Node}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, DateType}
import scala.beans.BeanProperty

class NodeTestDateToAge extends Node with Serializable{


  @BeanProperty var inputColName = ""
  @BeanProperty var yearsOutputColName = ""
  @BeanProperty var daysOutputColName = ""


  def this(i: Int, nme: String){
     this()
     id = i
     name = nme
   }

  def this(i: Int, nme: String, inputColName: String, yearsColName: String, daysColName: String ){
   this(i, nme)
    this.inputColName = inputColName
    this.yearsOutputColName = yearsColName
    this.daysOutputColName = daysColName
  }


  @Override
  override def execute (jobContext : JobContext) {


    val numDays =  datediff(current_date() , col(inputColName).cast(DateType))
    val numYears =  (numDays / 365 ).cast(IntegerType)
    val resultDF = dataFrame.withColumn(yearsOutputColName, numYears)
                              .withColumn(daysOutputColName, numDays)

    passDataFrameToNextNodesAndExecute(jobContext, resultDF)

  }

  override def getOutputSchema(workflow: Workflow, inputSchema: FireSchema): FireSchema = {

    inputSchema.addColumn(yearsOutputColName, FireSchema.Type.INTEGER, FireSchema.MLType.NUMERIC)
    inputSchema.addColumn(daysOutputColName, FireSchema.Type.INTEGER, FireSchema.MLType.NUMERIC)

    return inputSchema
  }

}




