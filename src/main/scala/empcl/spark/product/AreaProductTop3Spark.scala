package empcl.spark.product

import com.alibaba.fastjson.JSON
import empcl.conf.ConfigurationManager
import empcl.constants.Constants
import empcl.dao.factory.DaoFactory
import empcl.helper.JdbcPoolHelper
import empcl.utils.ParamUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable

/**
  * 需求：根据用户指定的日期范围，统计各个区域下的最热门的top3商品
  * 步骤：
  *   1）首先分别读取出sessionInfoDS，productInfoDS，cityInfoDS的数据
  *   2）统计出各个区域下的商品的点击次数
  *   3）使用开窗函数计算出各个区域下最热门的top3商品并保存到数据库中
  *
  */
object AreaProductTop3Spark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName(Constants.SPARK_APP_NAME_PRODUCT).setMaster("local[4]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")

    // 注册自定义函数
    sparkSession.udf.register("contactcity_id_name", ContactStringStringUDF, DataTypes.StringType)
    sparkSession.udf.register("group_concat_distinct", GroupContactDistinctUDF)
    sparkSession.udf.register("get_json_field", GetJsonFieldUDF, DataTypes.StringType)

    val task = DaoFactory.getTaskDao.findById(3)
    val param = task.taskParam
    val param_json = JSON.parseObject(param)
    val startDate = ParamUtils.getParam(param_json, "startDate").getOrElse("2015-11-24 21:50:56")
    val endDate = ParamUtils.getParam(param_json, "endDate").getOrElse("2025-11-24 21:50:56")

    import sparkSession.implicits._
    val userVistActionDF = sparkSession.read.parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_visit_action\\")
    val userInfoDF = sparkSession.read.parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_info")
    val productInfoDF = sparkSession.read.parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\product_info")
    userVistActionDF.createOrReplaceTempView("userVisitAction")
    userInfoDF.createOrReplaceTempView("userInfo")
    productInfoDF.createOrReplaceTempView("productInfo")

    val sql_query_session =
      s"""
         |select
         | city_id,
         | click_product_id as product_id
         |from
         | userVisitAction
         |where
         | action_time >= "$startDate"
         |and
         | action_time <= "$endDate"
         |and
         | click_product_id is not null
         |and
         | click_product_id != 'NULL'
         |and
         | click_product_id != 'null'
      """.stripMargin

    val sql_query_product =
      """
        |select
        | product_id,
        | product_name,
        | if(get_json_field(extend_info,"product_status") = 0,"自营产品","第三方产品") as product_status
        |from
        | productInfo
      """.stripMargin

    val sessionInfoDS = sparkSession.sql(sql_query_session).as[SessionInfo]
    val productInfoDS = sparkSession.sql(sql_query_product).as[ProductInfo]

    // 从数据库中查询出城市信息
    val map = new mutable.HashMap[String, String]()
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    val user = ConfigurationManager.getProperty(Constants.JDBC_USER)
    val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
    map.put("url", url)
    map.put("user", user)
    map.put("password", password)
    map.put("dbtable", "city_info")
    val cityInfoDS = sparkSession.read.format("jdbc").options(map).load().as[CityInfo]

    // 统计出各个区域下的商品的点击次数
    val product2CityInfoDF = sessionInfoDS.join(cityInfoDS, "city_id")
    product2CityInfoDF.createOrReplaceTempView("tmp_product_city_base")
    val sql_product_city =
      """
        |select
        | area,
        | product_id,
        | count(*) as click_count,
        | group_concat_distinct(contactcity_id_name(city_id,city_name,":")) as city_infos
        |from
        | tmp_product_city_base
        |group by
        | area,product_id
      """.stripMargin
    val productCityAreaDS = sparkSession.sql(sql_product_city).as[ProductCityArea]
    productCityAreaDS.createOrReplaceTempView("tmp_product_city_area")

    val sql_pca =
      """
        |select
        | area,
        | case
        |   when  area = '华东' OR  area = '华西' THEN 'A Level'
        |   when  area = '华南' OR  area = '华中' THEN 'B Level'
        |   when  area = '西北'  OR   area = '西南' THEN 'C Level'
        |   else 'D Level'
        |   end as area_level,
        | product_id,
        | city_infos,
        | click_count
        |from (
        |   select
        |     area,
        |     product_id,
        |     click_count,
        |     city_infos,
        |     row_number() OVER (PARTITION BY area ORDER BY click_count DESC) rank
        |    from
        |     tmp_product_city_area
        | ) t
        |where
        | rank <= 3
      """.stripMargin
    val productAreaTop3TmpDS = sparkSession.sql(sql_pca).as[ProductAreaTop3Tmp]
    val productAreaTop3DS = productAreaTop3TmpDS.join(productInfoDS, "product_id")
    // |product_id|area|area_level|city_infos|click_count|product_name|product_status|
    val insert_sql = "insert into area_top3_product values (?,?,?,?,?,?,?,?)"
    JdbcPoolHelper.getJdbcPoolHelper.execute(insert_sql, pstmt => {
      productAreaTop3DS.collect().foreach(row => {
        pstmt.setInt(1,task.taskId)
        pstmt.setString(2,row.getString(1))
        pstmt.setString(3,row.getString(2))
        pstmt.setString(4,row.getString(0))
        pstmt.setString(5,row.getString(3))
        pstmt.setString(6,String.valueOf(row.getLong(4)))
        pstmt.setString(7,row.getString(5))
        pstmt.setString(8,row.getString(6))

        pstmt.addBatch()
      })
    })

    sparkSession.stop()
  }

}

case class SessionInfo(city_id: String, product_id: String)

case class CityInfo(city_id: String, city_name: String, area: String)

case class ProductInfo(product_id: String, product_name: String, product_status: String)

case class ProductCityArea(area: String, product_id: String, click_count: Long, city_infos: String)

case class ProductAreaTop3Tmp(area: String, area_level: String, product_id: String, city_infos: String, click_count: String)