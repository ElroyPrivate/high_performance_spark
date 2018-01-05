package com.high_performance_Spark

//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object MainClass extends App{

  val sparkConf=new SparkConf().setMaster("local[*]").setAppName("JustForFun")
  val sc=new SparkContext(sparkConf)
  val sqlContext=new SQLContext(sc)

  val userData=Array(
    "2016-3-27,001,http://spark.apache.org/,1000",
    "2016-3-27,001,http://hadoop.apache.org/,1001",
    "2016-3-27,002,http://fink.apache.org/,1002",
    "2016-3-28,003,http://kafka.apache.org/,1020",
    "2016-3-28,004,http://spark.apache.org/,1010",
    "2016-3-28,002,http://hive.apache.org/,1200",
    "2016-3-28,001,http://parquet.apache.org/,1500",
    "2016-3-28,001,http://spark.apache.org/,1800"
  )

  val userDataRDD=sc.parallelize(userData)

  val userDataRDDRow=userDataRDD.map(row=>{
    val splited=row.split(",")
    Row(splited(0),splited(1).toInt,splited(2),splited(3).toInt)
  })
  val structType=StructType(Array(
    StructField("time",StringType,true),
    StructField("id",IntegerType,true),
    StructField("url",StringType,true),
    StructField("amount",IntegerType,true)
  ))

  val userDataDF=sqlContext.createDataFrame(userDataRDDRow,structType)
  userDataDF.groupBy("time").agg(Map("id"->"id1","amount"->"max"))
  //userDataDF.agg(max("time"),avg("amount"))
  //userDataDF.groupBy("time").agg('time',)).map(row=>Row(row(1),row(2))).co

  def generateData(n:Int):Array[Long]={
    def generateLong(i:Int):Long={
      i.toLong
    }
    Array.tabulate(n)(generateLong)
  }

  val datasource=generateData(10)

}
