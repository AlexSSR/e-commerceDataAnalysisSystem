package empcl

import java.util.Date

import empcl.conf.ConfigurationManager
import empcl.dao.factory.DaoFactory
import empcl.utils._

/**
  * @author : empcl
  * @since : 2018/8/8 22:41 
  */
object DateTest {

  def main(args: Array[String]): Unit = {

    /*val a = StringUtils.getFieldFromConcatString("searchKeywords=|clickCategoryIds=1,2,3","\\|","clickCategoryId")
    println(a.isEmpty)*/
    // searchKeywords=|clickCategoryIds=1,2,3
    /*val data = "searchKeywords=|clickCategoryIds=4"
    val dataField = "clickCategoryIds"
    val paramater = "searchKeywords=|clickCategoryIds1= 1,2,3,18,20 |clickCategoryIds2= "
    val startParamField = "clickCategoryIds1"
    val endParamField = "clickCategoryIds2"
//    val a = ValidUtils.between(data,dataField,paramater,startParamField,endParamField)
    val b = ValidUtils.in(data,dataField,paramater,startParamField)
    val c = ValidUtils.equals(data,dataField,paramater,endParamField)
    print(c)*/
//    println(ConfigurationManager.getProperty("a"))
    println(DaoFactory.getTaskDao.findById(1L))
    println(DaoFactory.getTaskDao.findById(2L))
    println(DaoFactory.getTaskDao.findById(3L))


  }

}
