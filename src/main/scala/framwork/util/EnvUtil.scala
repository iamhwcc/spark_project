package framwork.util

import org.apache.spark.SparkContext

object EnvUtil {
    private val scLocal = new ThreadLocal[SparkContext]()

    def putEnv(sc:SparkContext): Unit = {
        scLocal.set(sc)
    }

    def getEnv(): SparkContext = {
        scLocal.get()
    }

    def deleteEnv(): Unit = {
        scLocal.remove()
    }
}
