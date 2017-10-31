package partner

import kotlinx.coroutines.experimental.launch
import network.FollowerServer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.framework.recipes.cache.NodeCacheListener
import org.apache.zookeeper.CreateMode
import org.jetbrains.annotations.TestOnly
import redis.clients.jedis.Client
import redis.clients.jedis.Jedis
import java.net.InetSocketAddress
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue

/**
 * Follower节点会在Leader节点创建完/ready后
 * 添加自己的IP地址
 *
 * 有三个线程：
 *  ReceiveJobThread：在Full状态下，接收主线程的所有任务;在HALF状态下，仅接收select任务;在NON状态下，抛弃一切任务/反馈不接受
 *  RedisJobReceiverThread：在HALF或NON状态下，获取Redis消息队列的任务，加入堵塞队列。（由于能批量获取，减少网络开销）
 *  ExecuteJobThread：在HALF或NON状态下，执行从Redis扒下来的任务
 */
class Follower(var leader: InetSocketAddress?,
               private val jedis: Jedis,
               private val curator: CuratorFramework) : Partner{
    private val lock = CountDownLatch(1)
    private val key = ClientUtil.getServerPort()

    private val tasks = LinkedBlockingQueue<JobRequest>()

    @Volatile
    var working = true

    //启动两个线程
    private val receivedThread = ReceiveJobThread()
    private val executedThread = ExecuteJobThread()
    private val redisJobThread = RedisJobReceiverThread(jedis)


    private inner class ReceiveJobThread : Thread("Follower-Receiver"){
        override fun run() {
            val port = ClientUtil.getPort()
            FollowerServer(port).start()
            println("ReceiveJobThread Quit")
        }
    }

    private inner class RedisJobReceiverThread(val jedis: Jedis) : Thread("Redis-Job-Receiver"){
        override fun run() {
            val key = ClientUtil.getServerPort()
            while (working){
                val commands = jedis.brpop(5000,key)
                if ( commands != null && commands.size != 0){
                    for (c in commands){
                        val request = JobRequest.toJobRequest(c)
                        launch {
                            tasks.put(request)
                        }
                    }
                }
            }


        }
    }

    private inner class ExecuteJobThread : Thread("Follower-Executor"){
        override fun run() {
            while (working){
                val job = tasks.take()
                //TODO:这里是测试，记得改掉
                MySQLUtil.fakeExecutorSQL(job)
            }
            println("ExecuteJobThread Quit")
        }
    }


    //TODO:这个方法为了测试
    @TestOnly
    fun fakeInit(): Boolean{
        val r = ReceiveJobThread()
        val e = ExecuteJobThread()
        val redisThread = RedisJobReceiverThread(jedis)

        r.isDaemon = false
        e.isDaemon = false
        redisThread.isDaemon = false

        r.start()
        e.start()
        redisThread.start()

        return true
    }

    override fun init(): Boolean{
        watchLeaderReady()
        lock.await()
        curator.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                .forPath("/follower/${ClientUtil.getServerPort()}",
                        FollowerState.FULL.toByteArray())
        jedis.lpop(key)

        receivedThread.isDaemon = false
        executedThread.isDaemon = false
        redisJobThread.isDaemon = false


        receivedThread.start()
        executedThread.start()
        redisJobThread.start()

        return true
    }

    private fun watchLeaderReady(){
        val cache = NodeCache(curator,"/ready")
        cache.listenable.addListener(NodeCacheListener {
            val data: String = String(cache.currentData.data)
            setLeaderAddr(data)
            lock.countDown()
        })
        cache.start()
    }

    private fun setLeaderAddr(data: String){
        val aAndP = data.split(":")
        if (leader == null){
            leader = InetSocketAddress(aAndP[0],Integer.parseInt(aAndP[1]))
        }
    }


    override fun close() {
        val key = ClientUtil.getServerPort()
        jedis.del(key)
        curator.close()
        jedis.close()
        working = false
        receivedThread.interrupt()
    }
}
