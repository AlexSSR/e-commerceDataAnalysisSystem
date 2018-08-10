package empcl.helper

import java.sql.{Connection, PreparedStatement, ResultSet}

import empcl.conf.ConfigurationManager
import empcl.constants.Constants
import org.apache.commons.dbcp.BasicDataSource


/**
  * @author : empcl
  * @since : 2018/8/9 16:16 
  */
class JdbcPoolHelper extends Serializable {

  var queryResult: Option[Any] = _

  private val dataSource = try {

    val ds = new BasicDataSource
    val jdbc_driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)
    val jdbc_user = ConfigurationManager.getProperty(Constants.JDBC_USER)
    val jdbc_password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
    val jdbc_url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val jdbc_pool_initSize = ConfigurationManager.getInt(Constants.JDBC_POOL_INITSIZE)
    val jdbc_pool_maxActive = ConfigurationManager.getInt(Constants.JDBC_POOL_MAXACTIVE)
    val jdbc_pool_maxIdel = ConfigurationManager.getInt(Constants.JDBC_POOL_MAXIDEL)
    val jdbc_pool_maxWait = ConfigurationManager.getInt(Constants.JDBC_POOL_MAXWAIT)

    ds.setDriverClassName(jdbc_driver)
    ds.setUsername(jdbc_user)
    ds.setPassword(jdbc_password)
    ds.setUrl(jdbc_url)
    ds.setInitialSize(jdbc_pool_initSize)
    ds.setMaxActive(jdbc_pool_maxActive)
    ds.setMaxIdle(jdbc_pool_maxIdel)
    ds.setMaxWait(jdbc_pool_maxWait)

    ds
  } catch {
    case ex: Exception => throw ex
  }

  def getConnection: Connection = dataSource.getConnection

  def executeQuery(sql: String, params: Array[Any],addParam: (Array[Any],PreparedStatement) => Unit,parseResultSet: (ResultSet) => Option[Any]): Option[Any] = {
    val conn = getConnection
    val stmt = conn.prepareStatement(sql)
    try {
      addParam(params,stmt)
      val rs = stmt.executeQuery()
      parseResultSet(rs)
    } finally {
      if (!stmt.isClosed) stmt.close() // conn不能关闭，result以及stmt可以关闭
    }
  }

  def execute(sql: String, addParam: (PreparedStatement) => Unit): Array[Int] = {
    val conn = getConnection
    val stmt = conn.prepareStatement(sql)
    try {
      stmt.clearBatch()
      addParam(stmt)
      val result = stmt.executeBatch()
      result
    } finally {
      if (!stmt.isClosed) stmt.close()
    }
  }

}

object JdbcPoolHelper extends Serializable {
  var poolHelper: Option[JdbcPoolHelper] = None

  def getJdbcPoolHelper: JdbcPoolHelper = {
    poolHelper match {
      case Some(p) => p
      case None => poolHelper = Some(new JdbcPoolHelper) //只需要初始化一次就可以长久使用
    }
    poolHelper.get
  }
}
