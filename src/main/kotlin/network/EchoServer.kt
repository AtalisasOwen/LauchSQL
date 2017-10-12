package network

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel

import java.net.InetSocketAddress

/**
 * Created by owen on 17/9/19.
 */
class EchoServer(private val port: Int) {

    fun start() {
        val serverHandler = EchoServerHandler()
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

fun main(args: Array<String>){
    EchoServer(12321).start()
}
