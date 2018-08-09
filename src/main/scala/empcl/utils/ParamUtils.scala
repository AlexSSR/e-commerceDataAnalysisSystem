package empcl.utils

import com.alibaba.fastjson.JSONObject
import empcl.conf.ConfigurationManager
import empcl.constants.Constants

/**
  * @author : empcl
  * @since : 2018/8/9 15:18 
  */
object ParamUtils {

  /**
    * 从命令行参数中提取任务id
    * 本地使用taskType，远程用args
    * 不符合要求的参数格式返回-1L
    *
    * @param args 命令行参数
    * @param taskType 本地参数
    * @return 任务id
    */
  def getTaskIdFromArgs(args: Array[String], taskType: String): Long = {

    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) {
      ConfigurationManager.getLong(taskType)
    } else {
      if (args != null && args.length > 0) {
        args(0).toLong
      }else{
        -1L
      }
    }
  }

  /**
    * 从JSON对象中提取参数
    *
    * @param jsonObject JSON对象
    * @param field 需要提取的字段名
    * @return 参数
    */
  def getParam(jsonObject: JSONObject,field: String): Option[String] = {
    val fieldValues = jsonObject.getJSONArray(field)
    if(fieldValues != null && fieldValues.size() > 0) {
      Some(fieldValues.getString(0))
    }else{
      None
    }
  }
}