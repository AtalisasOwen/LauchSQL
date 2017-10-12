package context

import org.apache.curator.framework.CuratorFramework
import partner.Looker
import partner.Partner
import redis.clients.jedis.Jedis

/**
 * Created by owen on 17/10/10.
 *
 * 1.主节点需要知道所有从节点的ID,要先完成主节点的zk监控，再添加从节点
 * 2.主节点只需要完成半数的任务发布，记录下完成任务的，剩下的没完成的加入Redis
 * 3.从节点需要<1>从主节点接受任务<2>当状态为HALF时，还需要从Redis获取任务
 * 4.主节点还要把任务加进从节点对应的Redis列表
 * 5.从节点执行失败...不知道...
 *
 */



class Selection{

    val curator: CuratorFramework
    init {
        curator = ClientUtil.createCurator()
    }


    fun selectionAndWork(){
        val jedis: Jedis = ClientUtil.createJedis()
        var partner: Partner
        var looker: Looker = Looker(jedis, curator)
        val isLeader = looker.init()
        if (isLeader){
            partner = looker.turnToLeader()
        }else{
            partner = looker.turnToFollower()
        }


        partner.init()

    }

    fun close(){
        curator.close()
    }

}



