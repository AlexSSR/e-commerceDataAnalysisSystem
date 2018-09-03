package empcl.mock;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import empcl.utils.DateUtils;
import empcl.utils.StringUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;


/*
 * 模拟数据程序
 * @author: empcl
 * @date: 2018/5/11 23:25
 */
public class MockData {

    /*
     * 模拟数据
     * @param sc
     * @param sqlContext
     */
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("MockData").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[]{"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[]{"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            String userid = random.nextInt(10) + "";
            for (int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                for (int k = 0; k < random.nextInt(100); k++) {
                    String pageid = random.nextInt(10) + "";
                    String actionTime = baseActionTime + ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    String clickCategoryId = null;
                    String clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if ("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(10)];
                    } else if ("click".equals(action)) {
                        clickCategoryId = String.valueOf(random.nextInt(100));
                        clickProductId = String.valueOf(random.nextInt(100));
                    } else if ("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(100));
                        orderProductIds = String.valueOf(random.nextInt(100));
                    } else if ("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(100));
                        payProductIds = String.valueOf(random.nextInt(100));
                    }

                    Row row = RowFactory.create(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds, String.valueOf(random.nextInt(10)));
                    rows.add(row);
                }
            }
        }

        JavaRDD<Row> rowsRDD = sc.parallelize(rows);

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("session_id", DataTypes.StringType, true),
                DataTypes.createStructField("page_id", DataTypes.StringType, true),
                DataTypes.createStructField("action_time", DataTypes.StringType, true), // 时间
                DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
                DataTypes.createStructField("click_category_id", DataTypes.StringType, true),
                DataTypes.createStructField("click_product_id", DataTypes.StringType, true),
                DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
                DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
                DataTypes.createStructField("city_id", DataTypes.StringType, true)));

        Dataset<Row> df = sparkSession.createDataFrame(rowsRDD, schema);
        df.registerTempTable("user_visit_action");
        df.coalesce(1).write().mode(SaveMode.Overwrite).parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_visit_action");
//        df.show(1,false);
        //==================================================================
        rows.clear();
        String[] sexes = new String[]{"male", "female"};
        for (int i = 0; i < 100; i++) {
            String userid = i + "";
            String username = "user" + i;
            String name = "name" + i;
            String age = random.nextInt(60) + "";
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age,
                    professional, city, sex);
            rows.add(row);
        }

        rowsRDD = sc.parallelize(rows);

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.StringType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.StringType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> df2 = sparkSession.createDataFrame(rowsRDD, schema2);
        df2.coalesce(1).write().mode(SaveMode.Overwrite).parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\user_info");
        df2.registerTempTable("user_info");

        rows.clear();
        int[] productStatus = new int[]{0, 1};
        for(int i = 0; i < 100; i ++) {
            String productId = String.valueOf(Long.valueOf(i));
            String productName = "product" + i;
            String extendInfo = "{'product_status': " + productStatus[random.nextInt(2)] + "}";
            Row row = RowFactory.create(productId, productName, extendInfo);
            rows.add(row);
        }
        rowsRDD = sc.parallelize(rows);
        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.StringType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> df3 = sparkSession.createDataFrame(rowsRDD, schema3);
        df3.coalesce(1).write().mode(SaveMode.Overwrite).parquet("E:\\empcl\\spark-project\\data\\input\\parquet\\product_info");
        df3.registerTempTable("product_info");
    }

}

