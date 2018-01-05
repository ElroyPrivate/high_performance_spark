package com.high_performance_Spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object HappyPandas extends App{

  /**
    * Creates a SparkSession
    * @return
    */
  def sparkSession():SparkSession={
    val session=SparkSession.builder()
        .master("local[*]")
        .appName("HappyPandas")
        .getOrCreate()

    session
  }

  /**
    *
    * @param name name of panda
    * @param zip  zip code
    * @param pandaSize  size of panda in kg
    * @param age  age of panda
    */
  case class pandas(name:String,zip:String,pandaSize:Integer,age:Integer)

  def createAndPrintSchema()={
    val damao=RawPanda(1,"M1B,5K7","giant",true,Array(0.1,0.1))
    val pandaPlace=PandaPlace("toronto",Array(damao))
    val df=session.createDataFrame(Seq(pandaPlace))
    df.printSchema()
  }

  val session=sparkSession()
  //val sparkConf=new SparkConf().setMaster("local[*]").setAppName("JustForFun")
  //val sc=new SparkContext(sparkConf)
  //val sqLContext=new SQLContext(sc)
  //val pandas=loadDataSimpler(sc,session,"./source/rawpanda.json")
      //.map(_.toString)
  import session.implicits._
  val pandas1=session.sparkContext.textFile("/Users/houkailiu/Pandas.txt")
          .map(_.toString.split(","))
          .map(iter=>
            PandaPlace(iter(0),
            Array(RawPanda(iter(1).toLong,iter(2),iter(3),true,Array(iter(5).toDouble,iter(6).toDouble)))))
          .toDF()

  /**
    * extract panda's attribute from PandaPlace with exploding
    */
  val pandaInfo=pandas1.explode(pandas1("pandas")){
    case Row(pandas:Seq[Row])=>
      pandas.map{
        case (Row(
        id:Long,
        zip:String,
        pt:String,
        happy:Boolean,
        attrs:Seq[Double]
        ))=>
        RawPanda(id,zip,pt,happy,attrs.toArray)
      }
  }

  pandaInfo.select(
    (pandaInfo("attributes")(0)/pandaInfo("attributes")(1)
        .as("squishyness"))
  ).show()

  minMeanSizePerZip(pandaInfo)
  pandaInfo.orderBy(pandaInfo("pandaSize").asc,pandaInfo(""))

  val windowSpec=Window
    .orderBy(pandaInfo("name"))
    .partitionBy(pandaInfo("zip"))
    .rowsBetween(start= -10,end=10)

  //import sqLContext.implicits._
  val pandaRelativeSizeCol = pandaInfo("id")-
    org.apache.spark.sql.functions.avg(pandaInfo("id")).over(windowSpec)

  /*
    Creates sqlContext with an existing SparkContext.
   */
  def sqlContext(sc:SparkContext):SQLContext={
    val sqlContext=new SQLContext(sc)

    import sqlContext.implicits._
    sqlContext
  }

  /**
    * Orders pandas by size ascending and by age descending.
    * Pandas will be sorted by "size" first and if two pandas have the same "size"
    * will be sorted by "age".
    * @param pandas
    * @return
    */
  def orderPandas(pandas:DataFrame):DataFrame={
    pandas.orderBy(pandas("pandaSize").asc,pandas("age").desc)
  }

  def describePandas(pandas:DataFrame)={
    val df=pandas.describe()
    println(df.collect())
  }

  def cutLineage(df:DataFrame)={
    val sqlCtx=df.sqlContext
    val rdd=df.rdd

    rdd.cache()
    sqlCtx.createDataFrame(rdd,df.schema)
  }

  def loadDataSimpler(sc:SparkContext,session: SparkSession,path:String):DataFrame={

    val df1=session.read.json(path)

    val df2=session.read.format("json").option("samplingRatio","1.0").load(path)

    val jsonRDD=sc.textFile(path)

    val df3=session.read.json(jsonRDD)

    df1
  }

  def jsonLoadFromRDD(session: SparkSession,input:RDD[String]):DataFrame={
    val rdd:RDD[String]=input.filter(_.contains("panda"))
    val df=session.read.json(rdd)

    df
  }

  def joins(df1:DataFrame,df2:DataFrame):Unit={

    //tag::innerJoin[]
    df1.join(df2,df1("name") === df2("name"))

    //Inner join explicit
    df1.join(df2,df1("name") === df2("name"),"inner")

    //Left outer join explicit
    df1.join(df2,df1("name") === df2("name"),"left_outer")

    //Right outer join explicit
    df1.join(df2,df1("name") === df2("name"),"right_outer")

    //Left semi join explicit
    df1.join(df2,df1("name") === df2("name"),"left_semi")
  }

  /**
    *
    * @param place  name of place
    * @param pandaType  type of panda in this place
    * @param happyPandas  if panda is happy
    * @param totalPandas  the total number of pandas in this place
    */
  case class PandaInfo(place:String,
                       pandaType:String,
                       happyPandas: Int,
                       totalPandas:Int)

  def happyPandasPercentage(pandaInfo: DataFrame):DataFrame={
    pandaInfo.select(
      pandaInfo("place"),(pandaInfo("happyPandas")/pandaInfo("totalPandas")).as("percentHappy")
    )
  }

  def selfJoin(df:DataFrame):DataFrame={
    val sqlCtx=df.sqlContext
    import sqlCtx.implicits._

    val joined=df.as("a").join(df.as("b")).where($"a.name"===$"b.name")

    joined
  }

  /**
    * Encodes pandaType to Integer values instead of String values.
    *
    * @param pandaInfo the input DataFrame
    * @return Returns a DataFrame of pandaId and integer value for pandaType
    */

  def encodePandaType(pandaInfo: DataFrame):DataFrame={
    pandaInfo.select(pandaInfo("id"),
        (org.apache.spark.sql.functions.when(pandaInfo("pt") === "giant", 0).
        when(pandaInfo("pt") === "red", 1).
        otherwise(2)).as("encodedType")
    )
  }

  def minMeanSizePerZip(pandas:DataFrame):DataFrame={
    //pandas.agg(first(df("age")))
    pandas.groupBy(pandas("zip")).agg(org.apache.spark.sql.functions.min(pandas("id")),org.apache.spark.sql.functions.mean(pandas("id")))
  }

  def windowFunction(pandaInfo: DataFrame):WindowSpec={
    val windowSpec=Window
      .orderBy(pandaInfo("age"))
      .partitionBy(pandaInfo("zip"))
      .rowsBetween( start= -10,end=10)
    windowSpec
  }

  def fromDF(df:DataFrame):Dataset[RawPanda]={
    df.as[RawPanda]
  }
}