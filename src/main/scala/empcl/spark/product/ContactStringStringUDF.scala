package empcl.spark.product

import org.apache.spark.sql.api.java.UDF3

// 四个参数分别是 三个输入，最后一个是输出
object ContactStringStringUDF extends UDF3[String, String, String, String] {

  override def call(input1: String, input2: String, split: String): String = {
    input1 + split + input2
  }
}
