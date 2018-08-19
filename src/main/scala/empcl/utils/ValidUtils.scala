package empcl.utils

import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
  * @author : empcl
  * @since : 2018/8/9 13:42 
  */
object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    *
    * @param data            数据
    * @param startParamField 起始参数字段
    * @param endParamField   结束参数字段
    * @return 校验结果
    */
  def between(data: String, startParamField: String,
              endParamField: String): Boolean = {

    val _startParamField = startParamField.trim
    val _endParamField = endParamField.trim
    val _data = data.trim

    var flag = false

    if (StringUtils.isEmpty(_startParamField) || StringUtils.isEmpty(_endParamField)) {
      flag = true
    } else {
      if (StringUtils.isNotEmpty(_data)) {
        val df = _data.toInt
        val spf = _startParamField.toInt
        val epf = _endParamField.toInt
        if (df >= spf && df <= epf) { // data在两者之间
          flag = true
        } else {
          flag = false
        }
      } else {
        flag = false
      }
    }
    flag
  }

  /**
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * data(4) 在paramField(1,2,3,4,5）中
    *
    * @param datas      数据
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(datas: Any, paramField: String): Boolean = {

    var flag = false
    val _paramField = paramField.trim
    val outer = new Breaks
    val inner = new Breaks
    if (StringUtils.isEmpty(_paramField)) {
      flag = true
    } else {
      if (StringUtils.isNotEmpty(datas)) {
        val pfs = _paramField.split(",")
        val splitedData = datas.toString.split(",")
        outer.breakable(
          for (data <- splitedData) {
            inner.breakable(
              for (pf <- pfs) {
                if (pf.trim == data) {
                  flag = true
                  outer.break()
                }
              }
            )
          }
        )
      }
    }
    flag
  }

  /**
    * 校验数据中的指定字段是否相等
    *
    * @param data       数据
    * @param paramField 参数字段
    * @return 校验结果
    */
  def equals(data: String, paramField: String): Boolean = {

    var flag = false
    val _paramField = paramField.trim
    val _data = data.trim
    if (StringUtils.isEmpty(_paramField)) {
      flag = true
    } else {
      if (StringUtils.isNotEmpty(_data)) {
        if (_data.equals(_paramField)) {
          flag = true
        }
      }
    }
    flag
  }

}
