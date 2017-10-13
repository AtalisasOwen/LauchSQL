import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig
import java.util.*
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by owen on 17/10/9.
 */
class ClientUtil {

    companion object{
        var curatorClient: CuratorFramework? = null
        var redisClient: JedisPool? = null
        val atomicNumber = AtomicInteger()

        @Synchronized
        private fun createPool(){
            val config = JedisPoolConfig()
            config.maxIdle = 10
            config.maxWaitMillis = 5000
            TODO("这里还要根据配置来设定")
            redisClient = JedisPool(config,"localhost")
        }


        fun getUniqueID(): String{
            return "Server:${atomicNumber.getAndIncrement()}"
        }

        fun getPort(): Int{
            /*
            val s = getServerPort().split(":")
            val port = Integer.parseInt(s[1])
            return port
            */
            return 6666
        }

        fun getServerPort(): String{
            //return "localhost:8888"
            //TODO:要根据配置文件来改
            return "localhost:${Random().nextInt(50000)}"
        }

        @Synchronized
        fun createCurator(): CuratorFramework{
            if (curatorClient == null){
                curatorClient = CuratorFrameworkFactory.newClient("localhost:2181", ExponentialBackoffRetry(1000,3))
                curatorClient?.start()
            }

            return curatorClient!!
        }

        @Synchronized
        fun createJedis(): Jedis{
            if (redisClient == null){
                createPool()
            }
            return redisClient!!.resource
        }




    }




}