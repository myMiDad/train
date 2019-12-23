package com.tom.graph

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//共同好友案例1
object CommonFriend1 {
  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    //sc
    val sc = new SparkContext(conf)
    //创建点集合
    val pointRDD: RDD[(VertexId, String)] = sc.makeRDD(Seq(
      (1L, "zhangsanfeng"),
      (2L, "zhangcuishan"),
      (9L, "yinsusu"),
      (6L, "baimeiyingwang"),
      (133L, "zhangwuji"),
      (16L, "jinmaoshiwang"),
      (21L, "zishanlongwang"),
      (44L, "qingxifuwang"),
      (138L, "yangdengtian"),
      (5L, "meijueshitai"),
      (7L, "laoheshang"),
      (158L, "zhouzhirou")
    ))
    //创建边集合
    val edgeRDD: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1, 133, 0),
      Edge(6, 133, 0),
      Edge(2, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(21, 138, 0),
      Edge(44, 138, 0),
      Edge(5, 158, 0),
      Edge(7, 158, 0)
    ))
    edgeRDD
    //创建图对象
    val graph: Graph[String, Int] = Graph(pointRDD,edgeRDD)
    //调用连通图算法
    //connectedComponents是将点和边抽象的描述出一张图
    //vertices,每一个点都和本图中最小的点两两配对
    val ver: VertexRDD[VertexId] = graph.connectedComponents().vertices
    //翻转、聚合
//    ver.map(x=>(x._2,x._1.toString)).reduceByKey((x,y)=>x.concat("|").concat(y)).foreach(println(_))
//    ver.map(x=>(x._2,Set(x._1))).reduceByKey(_++_).foreach(println(_))
    //join
    ver.join(pointRDD).map{
      case (maxid,(minid,name)) => (minid,List(name))
    }.reduceByKey(_++_).foreach(println(_))
    //释放资源
    sc.stop()
  }
}
