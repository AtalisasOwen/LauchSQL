package network

import io.netty.bootstrap.Bootstrap
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.SimpleChannelInboundHandler
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.util.CharsetUtil
import partner.FollowerState
import partner.JobRequest
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created by owen on 17/10/11.
 */

class LeaderCommunicationHandler(val input: ByteBuf) : SimpleChannelInboundHandler<ByteBuf>() {
    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        ctx.writeAndFlush(input)
        //ctx.writeAndFlush(JobRequest(JobType.INSERT,"adasda"))
    }

    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }

    override fun channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) {
        println(msg.toString(CharsetUtil.UTF_8))
    }
}

class LeaderCommunicationClient(private val input: ByteBuf,
                                private val addr: InetSocketAddress) {
    @Throws(InterruptedException::class)
    fun start() {
        val group = NioEventLoopGroup()
        try {
            val b = Bootstrap()
            b.group(group)
                    .channel(NioSocketChannel::class.java)
                    .remoteAddress(addr)
                    .handler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(LeaderCommunicationHandler(input))
                        }
                    })

            val f = b.connect().sync()
            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully().sync()
        }
    }



}


class LeaderServerHandler(val follower: ConcurrentHashMap<InetSocketAddress, String>) : EchoServerHandler(){
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {
        val input = msg as ByteBuf

        var jobAssigned = 0

        for ((k,v) in follower){
            if (v == FollowerState.FULL){
                LeaderCommunicationClient(input,k).start()
                jobAssigned += 1
                break
            }
        }

        println("$jobAssigned follower has received the job")

    }
}

class LeaderServer(val port: Int, val follower: ConcurrentHashMap<InetSocketAddress, String>) {

    fun start() {
        val serverHandler = LeaderServerHandler(follower)
        //创建事件循环组
        val group = NioEventLoopGroup()
        val group2 = NioEventLoopGroup()
        try {
            //服务器启动器
            val bootstrap = ServerBootstrap()
            bootstrap.group(group, group2)
                    .channel(NioServerSocketChannel::class.java)
                    .localAddress(InetSocketAddress(port))
                    .childHandler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(serverHandler)
                        }
                    })
            val f = bootstrap.bind().sync()
            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully().sync()
        }
    }

}