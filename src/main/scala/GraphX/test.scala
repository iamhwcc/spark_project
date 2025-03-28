package GraphX

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * 数仓血缘，利用Spark GraphX进行血缘计算、血缘查询
 */

object test {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("GraphX")
        val sc = new SparkContext(conf)

        // 顶点 VertexRDD[(VertexId, String)]：表ID 和 表名
        val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
            (1L, "ods.ods_order"),
            (2L, "dwd.dwd_order_base"),
            (3L, "dws.dws_order_day"),
            (4L, "ads.ads_order_summary"),
            (5L, "rpt.rpt_order_dashboard")
        ))

        // 边 EdgeRDD：表示血缘方向，从上游到下游（正向依赖）
        val edges: RDD[Edge[String]] = sc.parallelize(Seq(
            Edge(1L, 2L, "depends_on"),
            Edge(2L, 3L, "depends_on"),
            Edge(3L, 4L, "depends_on"),
            Edge(4L, 5L, "depends_on")
        ))

        // 构建图
        val graph: Graph[String, String] = Graph(vertices, edges)

        // 初始化所有节点值，目标节点为 true，其它为 false
        val initialGraph: Graph[Boolean, String] = graph.mapVertices { case (id, _) => if (id == 5L) true else false }

        // 定义 Pregel 消息传播逻辑（向上游传播）
        val lineageGraph: Graph[Boolean, String] = initialGraph.pregel(false, Int.MaxValue, EdgeDirection.In)(
            (id, curNode, newNode) => curNode || newNode, // Vertex Program
            // id: 本节点id； curNode: 本节点的Boolean值； newNode: 新节点的Boolean值
            // attr || msg 如果本节点是true，新节点也是true，那就保持true
            // false || true = true   // 它就更新成 true（说明它是目标表的上游）

            // triplet.srcId	边的起点 ID（source）→ 上游节点
            // triplet.dstId	边的终点 ID（destination）→ 下游节点
            // triplet.srcAttr	上游节点当前的状态值（布尔）
            // triplet.dstAttr	下游节点当前的状态值（布尔）

            triplet => {
                // 此处是血缘回溯，所以dstAttr就是自身的Boolean，srcAttr是上游的Boolean
                if (triplet.dstAttr) {
                    Iterator((triplet.srcId, true)) // 将上游变true
                } else {
                    Iterator.empty
                }
            }, // Send Message
            (a, b) => a || b // Merge Message // 下游多个节点发来，进行合并
        )

        // 过滤出值为 true 的节点（即所有能到达目标节点的上游节点）
        val upstream: RDD[String] = lineageGraph.vertices.filter { case (id, v) => v } // 提取出为true的
            .join(vertices) // 加表名
            .map {
                case (id, (v, name)) => name // 输出表名
            }

        upstream.collect().foreach(println)
    }
}
