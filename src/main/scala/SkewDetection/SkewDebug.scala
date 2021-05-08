package SkewDetection

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskEnd}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SkewDebug(var sc: SparkContext) {
  val inputRecords = ArrayBuffer[Long]()
  val stageId = ArrayBuffer[Long]()
  val callSites = ArrayBuffer[String]()
  val taskIdPerLine  = ArrayBuffer[Long]()
  var taskId = 1

  val mem_stage_id = ArrayBuffer[Long]()
  val mem_task_id = ArrayBuffer[Long]()
  val mem_time = ArrayBuffer[Long]()
  val comp_time = ArrayBuffer[Long]()
  val shuffle_rpr= ArrayBuffer[Long]()
  val mem_executor_id  =ArrayBuffer[String]()


  val hash_map = new mutable.HashMap[Long, mutable.Set[Long]] with mutable.MultiMap[Long, Long]
  val comp_map = new mutable.HashMap[Long, mutable.Set[Long]] with mutable.MultiMap[Long, Long]
  val data_map = new mutable.HashMap[Long, mutable.Set[Long]] with mutable.MultiMap[Long, Long]
  val location_map = new mutable.HashMap[Long, mutable.Set[String]] with mutable.MultiMap[Long, String]

  val memSet: mutable.HashSet[Long] = mutable.HashSet()
  val dataSet: mutable.HashSet[Long] = mutable.HashSet()

  val finalStageId = ArrayBuffer[Long]()
  val finalCallSites = ArrayBuffer[String]()

  sc.addSparkListener(new SparkListener() {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {


      if (taskEnd.taskMetrics.inputMetrics != None) {

        mem_stage_id+=taskEnd.stageId
        mem_time += taskEnd.taskMetrics.jvmGCTime
        mem_task_id+=taskEnd.taskInfo.taskId
        mem_executor_id+=taskEnd.taskInfo.host
        comp_time+=taskEnd.taskMetrics.executorRunTime
        shuffle_rpr+=taskEnd.taskMetrics.shuffleReadMetrics.totalBytesRead



        for(i <- 0 to mem_time.length-1) {

          if(mem_time(i)==0) mem_time(i) = 1

          hash_map.addBinding(mem_stage_id(i),mem_time(i))

        }
        for(i <- 0 to comp_time.length-1) {

          if(comp_time(i)==0) comp_time(i) = 1

          comp_map.addBinding(mem_stage_id(i),comp_time(i))

        }
        for(i <- 0 to shuffle_rpr.length-1) {

          if(shuffle_rpr(i)==0) shuffle_rpr(i) = 1

          data_map.addBinding(mem_stage_id(i),shuffle_rpr(i))

        }
      }

    }



    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {

      jobStart.stageInfos.foreach(info => {


        info.rddInfos.foreach(rdd => {


          callSites+=rdd.callSite
          stageId+=info.stageId
          location_map.addBinding(info.stageId,rdd.callSite)


        })
      })
    }

  })

  def printLog(): Unit = {
    println("Task Data Log")
    var execId = 0;

    for (i <- 0 to callSites.length - 1) {
      val temp = callSites(i).split(" ")
      var key = temp(0)
      if(key !="textFile" && key!="repartition"){
        finalStageId+=stageId(i)
        finalCallSites+=callSites(i)
      }

    }

    for(i<- 0 to mem_stage_id.length-1){
      println("Stage Id : "+ mem_stage_id(i)+" Task ID "+ mem_task_id(i)+" Shuffle Data Read "+ shuffle_rpr(i)+" GC Time : " + mem_time(i) + " Duration "+ comp_time(i))
    }
    for(i<- 0 to 5){
      println()
    }


    for (pair <- data_map){

      val set = pair._2
      var max = set.max
      var min = set.min
      var mem_metric = max/min
      if(mem_metric>=10) {
        if(dataSet(pair._1)==false){
          dataSet+=pair._1
          println("Data Skew at Stage " + pair._1+" | ")

          val task_id = ArrayBuffer[Long]()
          var max_gc_time = -1
          for (i <- 0 to mem_stage_id.length - 1) {

            if (mem_stage_id(i) == pair._1) {
              max_gc_time = Math.max(max_gc_time.toInt, shuffle_rpr(i).toInt)

            }
          }
          for (i <- 0 to mem_task_id.length - 1) {
            if (shuffle_rpr(i) == max_gc_time) {
              println("High workload at Task ID " + mem_task_id(i) + "  |  Executor ID : " + mem_executor_id(i) + " | Shuffle Read/Record : "+ shuffle_rpr(i)+" bytes")
            }
          }

          val locations = location_map(pair._1)
          for(line <- locations){
            println("Possible code location of skew is : " + line)
          }
          println()
        }
      }

    }
    for (pair <- hash_map){

      val set = pair._2
      var max = set.max
      var min = set.min
      var mem_metric = max/min
      if(mem_metric>=10) {
        if(dataSet(pair._1)==false){
          memSet+=pair._1
          println("Memory Skew at Stage " + pair._1+" | ")

          val task_id = ArrayBuffer[Long]()
          var max_gc_time = -1
          for (i <- 0 to mem_stage_id.length - 1) {

            if (mem_stage_id(i) == pair._1) {
              max_gc_time = Math.max(max_gc_time.toInt, shuffle_rpr(i).toInt)

            }
          }
          for (i <- 0 to mem_task_id.length - 1) {
            if (shuffle_rpr(i) == max_gc_time) {
              println("High Garbage Collection at Task ID " + mem_task_id(i) + "  |  Executor ID : " + mem_executor_id(i) + " | GC Time : "+ mem_time(i)+" ms")
            }
          }

          val locations = location_map(pair._1)
          for(line <- locations){
            println("Possible code location of skew is : " + line)
          }
          println()
        }
      }

    }

    for (pair <- comp_map){

      val set = pair._2
      var max = set.max
      var min = set.min
      var mem_metric = max/min
      if(mem_metric>=10) {
        if(memSet(pair._1)==false && dataSet(pair._1)==false){
          println("Computation Skew at Stage " + pair._1+" | ")

          val task_id = ArrayBuffer[Long]()
          var max_gc_time = -1
          for (i <- 0 to mem_stage_id.length - 1) {

            if (mem_stage_id(i) == pair._1) {
              max_gc_time = Math.max(max_gc_time.toInt, comp_time(i).toInt)

            }
          }
          for (i <- 0 to mem_task_id.length - 1) {
            if (comp_time(i) == max_gc_time) {
              println("High computation complexity at Task ID " + mem_task_id(i) + "  |  Executor ID : " + mem_executor_id(i) + " | Task Duration : "+ comp_time(i)+" ms")
            }
          }

          val locations = location_map(pair._1)
          for(line <- locations){
            println("Possible code location of skew is : " + line)
          }
          println()
        }

      }

    }

  }
}

