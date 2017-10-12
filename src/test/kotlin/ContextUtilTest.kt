import context.Selection
import org.junit.Test
import partner.Follower
import partner.Looker
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Created by owen on 17/10/10.
 */
class ContextUtilTest {

    @Test
    fun testLeader(){

        val ex = Executors.newFixedThreadPool(5)
        val s = Selection()

        for (i in 1..5){
            ex.submit {
                s.selectionAndWork()
            }
        }

        ex.awaitTermination(5,TimeUnit.SECONDS)
        ex.shutdown()
        s.curator.close()


    }

    @Test
    fun testFollower(){
        val jedis = ClientUtil.createJedis()
        val curator = ClientUtil.createCurator()
        val follower = Follower(null,jedis,curator)
        follower.init()
    }

}
