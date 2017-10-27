import org.junit.Test
import redis.clients.jedis.Jedis
import  org.junit.Assert.*;
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Created by owen on 17/10/9.
 */
class JedisTest {

    infix fun print(any: Any){
        println(any)
    }


    @Test
    fun testJedis(){
        val client = Jedis("localhost",6379)
        client.set("hello","world")
        client.lpush("Server:1","")
        val result = client["hello"]
        assertEquals(result,"world")

        Thread.sleep(15000)

        this print client.get("hello")

        Thread.sleep(5000)

        client.close()
    }

    @Test
    fun testJedisList(){
        val client = ClientUtil.createJedis()
        val executor = Executors.newFixedThreadPool(5)
        executor.submit {
            val s = ClientUtil.getServerPort()
            client.lpush(s, "")
        }



        executor.awaitTermination(5,TimeUnit.SECONDS)
        executor.shutdown()
        client.close()

    }

}