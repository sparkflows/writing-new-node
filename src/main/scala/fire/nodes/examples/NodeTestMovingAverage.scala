package fire.nodes.examples

import fire.context.JobContext
import fire.workflowengine.{FireSchema, Node, Workflow}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col}

import scala.beans.BeanProperty

class NodeTestMovingAverage extends Node{

  @BeanProperty var windowStart:Long = -1L
  @BeanProperty var windowEnd:Long = 1L
  @BeanProperty var inputCol:String = _
 // @BeanProperty var orderBy:String = _
  //@BeanProperty var partitionBy: String = _

  def this(i: Int, nme: String){
    this()
    id = i
    name = nme
  }


  @Override
  override def execute(jobContext: JobContext): Unit = {

    dataFrame.show(false)


    val resultDF = dataFrame.withColumn(s"ma_$inputCol", avg(col(inputCol)).over(Window.orderBy(col(inputCol))rangeBetween(windowStart, windowEnd)))

    resultDF.show(false)
    passDataFrameToNextNodesAndExecute(jobContext, resultDF)

  }


  override def getOutputSchema(workflow: Workflow, inputSchema: FireSchema): FireSchema = {

    inputSchema.addColumn(s"ma_$inputCol", FireSchema.Type.DOUBLE ,FireSchema.MLType.NUMERIC)

    return inputSchema
  }

}
