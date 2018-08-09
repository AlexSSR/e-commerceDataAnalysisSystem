package empcl.utils

import scala.util.control.Breaks._
/**
  * @author : empcl
  * @since : 2018/8/9 13:42 
  */
object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * "A=1" 中的1 位于"B=1|C=2"中的1和2之间
    *
    * @param data            数据
    * @param dataField       数据字段
    * @param parameter       参数
    * @param startParamField 起始参数字段
    * @param endParamField   结束参数字段
    * @return 校验结果
    */
  def between(data: String, dataField: String,
              parameter: String, startParamField: String,
              endParamField: String): Boolean = {
    var flag = false
    val startParamFieldOpt = StringUtils.getFieldFromConcatString(parameter, "\\|", startParamField)
    val endParamFieldOpt = StringUtils.getFieldFromConcatString(parameter, "\\|", endParamField)
    if (startParamFieldOpt.isEmpty || endParamFieldOpt.isEmpty) {
      flag = true
    } else {
      val dataFieldStr = StringUtils.getFieldFromConcatString(data, "\\|", dataField)
      if (dataFieldStr.isDefined) {
        val df = dataFieldStr.get.toInt
        val spf = startParamFieldOpt.get.toInt
        val epf = endParamFieldOpt.get.toInt
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
    * 4 在（1,2，3,4,5）中
    *
    * @param data       数据
    * @param dataField  数据字段
    * @param parameter  参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def in(data: String, dataField: String, parameter: String, paramField: String): Boolean = {

    var flag = false
    val paramFieldOpt = StringUtils.getFieldFromConcatString(parameter, "\\|", paramField)
    if (paramFieldOpt.isEmpty){
      flag = true
    }else{
      val pfs = paramFieldOpt.get.split(",")
      val df = StringUtils.getFieldFromConcatString(data,"\\|",dataField)
      if(df.isDefined){
        breakable(
          for(pf <- pfs) {
            if(pf.toInt == df.get.toInt ){
              flag = true
              break()
            }
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
    * @param dataField  数据字段
    * @param parameter  参数
    * @param paramField 参数字段
    * @return 校验结果
    */
  def equals(data: String,dataField: String,parameter: String,paramField: String): Boolean = {

    var flag = false
    val paramFieldOpt = StringUtils.getFieldFromConcatString(parameter,"\\|",paramField)
    if(paramFieldOpt.isEmpty){
      flag = true
    }else{
      val dataFieldOpt = StringUtils.getFieldFromConcatString(data,"\\|",dataField)
      if(dataFieldOpt.isDefined) {
        val dataFieldValue = dataFieldOpt.get
        val paramFieldValue = paramFieldOpt.get
        if(dataFieldValue.equals(paramFieldValue)){
          flag = true
        }
      }
    }
    flag
  }

}
