package Examples

import Examples.Airport.temp_dir
import org.apache.spark.{SparkConf, SparkContext, rdd}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import scalaj.http.{Http, HttpOptions}
import org.apache.spark.sql.types.{StructType,StructField,StringType,IntegerType,DoubleType}
import org.apache.spark.Logging
import org.apache.log4j.{Level,Logger}

case class Json(labels:Array[String],values:Array[Int])
object TwitterAnalysis extends App {

  def aggregate_tags(num_values:Seq[(Int)],total_values:Option[(Int)]):Option[(Int)] ={
    var result:Option[(Int)]=null
    if(num_values.isEmpty)
      {
        result=Some(total_values.get)
      }
    else
    {
      num_values.foreach { x =>
        if (total_values.isEmpty) {
          result = Some(x)
        }
        else {
          result = Some(x + total_values.get)
        }
      }
    }
    result
  }

  val conf=new SparkConf().setAppName("twitter").setMaster("local[2]").set("spark.executor.memory","2g").set("spark.sql.shuffle.partitions","2")
  val sc=SparkContext.getOrCreate(conf)
  val scc=new StreamingContext(sc,Seconds(5))
  Logger.getRootLogger.setLevel(Level.WARN)

  //val sp=SparkSession.builder().appName("Spark-Twitter").master("local[2]").config("spark.sql.warehouse.dir",temp_dir).getOrCreate()
  val sp =new SQLContext(sc)
  scc.checkpoint("D:\\Edureka\\Twitter")
  System.setProperty("twitter4j.oauth.consumerKey","MfZcOdDb7173MvUYSdoPGGarr")
  System.setProperty("twitter4j.oauth.consumerSecret","G8IWgxieJVf10NtF5HE2OrAhxvc6k7Kut2rRAdrgQsdI2WfJIv")
  System.setProperty("twitter4j.oauth.accessToken","293484958-KitSyoDt9I8jj6BvxoqrkJ0n9vYMddjUWgaUnXCV")
  System.setProperty("twitter4j.oauth.accessTokenSecret","xWQi1map071dgcESqYr5CU539q5uNzeHG4xHKy2sRbdXS")
  val stream=TwitterUtils.createStream(scc,None)
  val tags=stream.flatMap{x=>x.getHashtagEntities.map(_.getText())}


  val total=tags.map(x=>(x,1)).updateStateByKey(aggregate_tags)
  total.foreachRDD(rdd=>rdd.saveAsTextFile("D:\\Edureka\\twitter_count.txt"))
 import sp.implicits._
  val schema=StructType(Array(StructField("hashtag",StringType,false),StructField("hashcount",IntegerType,false)))
 total.foreachRDD { rdd =>
    val rowRDD=rdd.map{x => Row(x._1,x._2)}
    sp.createDataFrame(rowRDD,schema).registerTempTable("hashtable")
    val hashdf=sp.sql("SELECT hashtag,hashcount FROM hashtable ORDER  BY hashcount DESC LIMIT 10")
    val labels=hashdf.select("hashtag").rdd.collect().map(_(0).toString()).toList
    val values=hashdf.select("hashcount").rdd.collect().map(_(0)).toList
    val json_string="""{"labels":"""+labels+""","values":"""+values+"""}"""
    val result=Http("http://localhost:5000/update").postForm(Seq("labels"->labels.toString(),"values"->values.toString())).header("Content-Type", "application/json")
    .header("Charset", "UTF-9")
    .option(HttpOptions.readTimeout(10000)).asString

  }
  tags.countByValue().foreachRDD{
    rdd=>
      val now=org.joda.time.DateTime.now()
      rdd.sortBy(_._2).map(x=>(x,now)).saveAsTextFile("D:\\Edureka\\twitter.txt")
  }
  scc.start()
  scc.awaitTermination()
}

