package com.javartisan.spark

package com.javartisan.spark

import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.launcher.SparkLauncher


/**
  * @author ${user.name}
  */
object SparkSQLAction {

  case class User(username: String, sex: String, addr: String) extends Serializable

  def main(args: Array[String]) {

    val env= new util.HashMap[String,String]()
    val launcher = new SparkLauncher(env)

    val users1 = Array(
      User("zs", "boy", "bj"),
      User("lw", "girl", "ln")
    )


    val users2 = Array(
      User("w2", "boy", "jl"),
      User("z6", "girl", "nj")
    )
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-local-sql")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._
    val usersDF = sc.parallelize(users1).toDF
    val usersDF1 = sc.parallelize(users2).toDF

    usersDF.createTempView("utab1")
    usersDF1.createTempView("utab2")


    spark.sql("select u2.* from utab1 u1 left join utab2 u2 on u1.username=u2.username").show(10)


    sc.stop()
    spark.stop()

  }

}

