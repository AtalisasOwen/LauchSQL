package network

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.CharsetUtil
import partner.JobRequest
import partner.JobType
import java.net.InetSocketAddress
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created by owen on 17/10/11.
 *接收Leader的请求，并串行执行
 */
class FollowerServerHandler() : EchoServerHandler() {

    fun toJobRequest(sql: String): JobRequest{
        //TODO(没那么简单。。。)
        return JobRequest(JobType.INSERT,sql)
    }

    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        val input = msg as ByteBuf
        val sql = input.toString(CharsetUtil.UTF_8)
        val req = toJobRequest(sql)

        //TODO("这里应该直接执行的，不应该放进队列")


        val su = "Success".toByteArray()
        val b = Unpooled.copiedBuffer(su)

        ctx.write(b)
    }
}

class FollowerServer(private val port: Int) {

    fun start() {
        val serverHandler = FollowerServerHandler()
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
