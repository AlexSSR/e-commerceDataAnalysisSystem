package empcl.spark.product

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.api.java.UDF2

object GetJsonFieldUDF extends UDF2[String, String, String] {

  override def call(json: String, field: String): String = {
      val jsonParse = JSON.parseObject(json)
      jsonParse.getString(field)
  }

}
