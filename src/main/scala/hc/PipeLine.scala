package hc
import SkewDetection.SkewDebug
import org.apache.spark.executor.InputMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
object PipeLine {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.set("spark.executor.memory", "512m")
    conf.setAppName("Skew Detection")

    val sc = new SparkContext(conf)

    //pass spark context to skewDebug constructor
    val skewDebug = new SkewDebug(sc)


    val rdd1 = sc.textFile("C:/Users/hemay/Desktop/hw3_bde/cs5614-hw-master/data/ticket_flights.csv")

    val apf = rdd1.map(f => {(f.split(",")(1), f.split(",")(3))})

    val apf2 = apf.map(f => {(f._1, f._2.toInt)})

    val totalfairperflight = apf2.reduceByKey((a, b) => {(a + b)})





    //memorySkew UDF
    def memSkew_udf(f: (String, Int)): (String,String) = {
      val result = (f._1, f._2.toString)
      if (result._1 == "32094") {
        //MEMORY SKEW
        for (i<- 1 to 10000){
          for (j<- 1 to 1000) {
            val new_array = ArrayBuffer[Int]()
            new_array += i
            val hashMap: mutable.HashMap[String, Int] = mutable.HashMap()
            hashMap += ("2" -> 3)
          }
        }
      }
      return result
    }
    val result = totalfairperflight.map(memSkew_udf)
    result.collect()



    //DataSkew Simulation
    val data =  ArrayBuffer[Long]()
    for(i <- 0 to 1000){
      data+=1
    }

    for(i <- 0 to 20){
      data+=0
    }
    for(i <- 0 to 100){
      data+=2
    }

    val data2 =  ArrayBuffer[Long]()
    for(i <- 0 to 1000){
      data2+=1
    }
    for(i <- 0 to 100){
      data2+=2
    }
    for(i <- 0 to 1){
      data2+=0
    }


    val distData1 = sc.parallelize(data.toSeq)
    val distData2 = sc.parallelize(data2.toSeq)

    val pairs1 = distData1.map(s => (s, 1)).repartition(6)
    val pairs2 = distData2.map(s => (s, 2)).repartition(6)
    val joined = pairs1.join(pairs2)
    val inter = joined.repartition(6).map(f=>{
      (f._1, f._2._1)
    })
    val reducedRDD = inter.reduceByKey((a,b)=>{
      (a+b)
    })


    //computation skew
    def comp_udf(f: (Long, Int)): (Long,Int) = {
      val result = (f._1, f._2)
      if (result._1 == 1) {
        //COMPUTATION SKEW
        Thread.sleep(4000)
      }
      return result
    }

    val result_dataSkew = reducedRDD.repartition(2).map(comp_udf)
    result_dataSkew.collect().foreach(println)








    skewDebug.printLog()

    val rawInput = StdIn.readLine()


  }
}

