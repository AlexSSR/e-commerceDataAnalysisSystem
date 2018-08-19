package empcl.utils

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

/**
  * 字符串工具类
  *
  * @author : empcl
  * @since : 2018/8/9 10:53 
  */
object StringUtils {

  // 判断字符串是否为空
  def isEmpty(str: Any): Boolean = {
    str == null || "".equals(str)
  }

  // 判断字符串是否不为空
  def isNotEmpty(str: Any): Boolean = {
    !isEmpty(str)
  }

  // 截断字符串两侧的逗号
  def trimComma(str: String): String = {
    var result = str
    if (result.startsWith(","))
      result = str.substring(1)
    if (result.endsWith(","))
      result = result.substring(0, result.length - 1)
    result
  }

  // 补全两位数字
  def fulfill(str: String): String = {
    if (str.length == 2) {
      str
    } else {
      "0" + str
    }
  }

  /**
    * 从拼接的字符串中提取字段
    *
    * @param str       字符串
    * @param delimiter 分隔符
    * @param field     字段
    * @return 字段值
    */
  def getFieldFromConcatString(str: String,
                               delimiter: String,
                               field: String): Option[String] = {
    // searchKeywords=|clickCategoryIds=1,2,3
    var result = ""
    val fields = str.split(delimiter)
    breakable(
      for (f <- fields) {
        val contactFields = f.split("=")
        if (contactFields.length == 2) {
          val fieldName = contactFields(0).trim
          val fieldValue = contactFields(1).trim
          if (field.equals(fieldName)) {
            result = fieldValue
            break()
          }
        }
      }
    )
    if (result.isEmpty) None else Some(result)
  }

  /**
    * 从拼接的字符串中给字段设置值
    *
    * @param str           字符串
    * @param delimiter     分隔符
    * @param field         字段名
    * @param newFieldValue 新的field值
    * @return 返回是否成功改动字段
    */
  def setFieldInConcatString(str: String,
                             delimiter: String,
                             field: String,
                             newFieldValue: String
                            ): String = {
    val fields = str.split(delimiter)
    var index1 = 0
    breakable(
      for(f <- fields) {
        val fieldName = f.split("=")(0)
        if(field.equals(fieldName)) {
          val contactField = fieldName+"="+newFieldValue
          fields(index1) = contactField
          break()
        }
        index1 = index1 + 1
      }
    )
    val fieldsBuffer = new ArrayBuffer[String]
    var index2 = 0
    val len = fields.length
    while (index2 < len){
//      fieldsBuffer.append(fields(index2))
      fieldsBuffer += fields(index2)
      if(index2 != len - 1) {
        fieldsBuffer.append("|")
      }
      index2 = index2 + 1
    }
    fieldsBuffer.mkString("")
  }
}
