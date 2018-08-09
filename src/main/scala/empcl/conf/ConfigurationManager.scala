package empcl.conf

import org.apache.commons.configuration.PropertiesConfiguration

/**
  * @author : empcl
  * @since : 2018/8/9 10:15 
  */

object ConfigurationManager {

  private val conf = new PropertiesConfiguration
  conf.load("my.properties")

  // 获取指定key的value
  def getProperty(key: String): String = {
    conf.getString(key)
  }

  // 获取整数类型的配置项
  def getInt(key: String): Int = {
    conf.getInt(key)
  }

  // 获取布尔类型的配置项
  def getBoolean(key: String): Boolean = {
    conf.getBoolean(key)
  }

  // 获取Long类型的配置项
  def getLong(key: String): Long = {
    conf.getLong(key)
  }

}
