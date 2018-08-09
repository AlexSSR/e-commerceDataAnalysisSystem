package empcl.dao.impl

import java.sql.{PreparedStatement, ResultSet}

import empcl.cases.Task
import empcl.dao.ITaskDao
import empcl.helper.JdbcPoolHelper

/**
  * @author : empcl
  * @since : 2018/8/9 17:35 
  */
class TaskDaoImpl extends ITaskDao {

  /**
    * 根据主键查询任务
    *
    * @param taskId 主键
    * @return 任务
    */
  override def findById(taskId: Long): Task = {

    val sql = "select * from task where task_id = ?"
    val queryResult = JdbcPoolHelper.getJdbcPoolHelper.executeQuery(sql, taskId+"",addParam,parseResultSet)
    if(queryResult.isDefined){
      queryResult.get.asInstanceOf[Task]
    }else{ // 未查到的task中的字段全部记为0
      Task(0,"0","0","0","0","0","0","0")
    }
  }

  val addParam = (param: String,stmt: PreparedStatement) => {
    stmt.setLong(1,param.toLong)
  }

  val parseResultSet = (rs: ResultSet) => {
    if (rs.next()) {
      val taskId = rs.getLong(1)
      val taskName = rs.getString(2);
      val createTime = rs.getString(3);
      val startTime = rs.getString(4);
      val finishTime = rs.getString(5);
      val taskType = rs.getString(6);
      val taskStatus = rs.getString(7);
      val taskParam = rs.getString(8);
      Some(Task(taskId,taskName,createTime,startTime,finishTime,taskType,taskStatus,taskParam))
    }else{
      None
    }
  }

}
