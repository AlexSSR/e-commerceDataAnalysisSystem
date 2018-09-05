package empcl.spark.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object GroupContactDistinctUDF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType) :: Nil)

  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType) :: Nil)

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val cityInfo = input.getString(0)
    var bufferStr = buffer.getString(0)
    if (!bufferStr.contains(cityInfo)) {
      if ("".equals(bufferStr)) {
        bufferStr += cityInfo
      } else {
        bufferStr = bufferStr + "," + cityInfo
      }
    }
    buffer(0) = bufferStr
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferStr1 = buffer1.getString(0)
    val bufferStr2 = buffer2.getString(0)
    val bufferStrSplitArr = bufferStr2.split(",")
    for (bss <- bufferStrSplitArr) {
      if (!bufferStr1.contains(bss)) {
        if ("".equals(bufferStr1)) {
          bufferStr1 += bss
        } else {
          bufferStr1 = bufferStr1 + "," + bss
        }
      }
    }
    buffer1(0) = bufferStr1
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }

}
