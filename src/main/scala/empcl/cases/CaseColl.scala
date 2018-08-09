package empcl.cases

/**
  * @author : empcl
  * @since : 2018/8/9 17:32 
  */
case class Task(
                 taskId: Long,
                 taskName: String,
                 createTime: String,
                 startTime: String,
                 finishTime: String,
                 taskType: String,
                 taskStatus: String,
                 taskParam: String
               ) extends Serializable



