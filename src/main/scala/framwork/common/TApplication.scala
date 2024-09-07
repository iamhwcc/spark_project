package framwork.common

import framwork.controller.WordCountController
import framwork.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {
    //启动程序
    //只要是一个Application，就可以用这个特质来启动程序

    def start(master:String="local[*]",appName:String="WordCountApplication")(fun: => Unit): Unit = {
        val conf = new SparkConf().setAppName(appName).setMaster(master)
        val sc = new SparkContext(conf)
        //将环境放入ThreadLocal中
        EnvUtil.putEnv(sc)
        //抽象控制
        try{
            //控制层代码块
            fun
        }catch {
            case ex => println(ex.getMessage)
        }

        sc.stop()
        //程序结束，删除环境
        EnvUtil.deleteEnv()
    }
}
