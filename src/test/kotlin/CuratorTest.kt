import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.ChildData
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.framework.recipes.cache.NodeCacheListener
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import  org.junit.Assert.*;
import org.junit.Test

/**
 * Created by owen on 17/10/9.
 */

class CuratorTest {

    @Test
    fun testCurator(){
        val client = CuratorFrameworkFactory.newClient("localhost:2181",ExponentialBackoffRetry(1000,3))
        client.start()
        client.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/test","qqww".toByteArray())
        Thread.sleep(1000)
        val data = client.data.forPath("/test")
        assertEquals(String(data),"qqww")
        client.close()
    }

    @Test
    fun testDeleteWatcher(){
        val PATH = "/master"

        val client = CuratorFrameworkFactory.newClient("localhost:2181",ExponentialBackoffRetry(1000,3))
        client.start()

        client.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(PATH,"localhost:2112".toByteArray())

        val cache = NodeCache(client,PATH)
        cache.listenable.addListener(NodeCacheListener {
            val data: ChildData? = cache.currentData
            if (data == null){
                println("delete")
            }else{
                println("data changed")
            }
        })
        cache.start()
        Thread.sleep(500)
        client.setData()
                .forPath(PATH,"localhost:21231".toByteArray())

        Thread.sleep(500)

        client.delete()
                .forPath(PATH)

        Thread.sleep(3000)

    }
}