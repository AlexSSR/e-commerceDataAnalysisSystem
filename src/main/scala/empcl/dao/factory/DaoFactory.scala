package empcl.dao.factory

import empcl.dao.ITaskDao
import empcl.dao.impl.TaskDaoImpl

/**
  * @author : empcl
  * @since : 2018/8/9 17:34 
  */
object DaoFactory {

  def getTaskDao: ITaskDao = {
    new TaskDaoImpl
  }

}
