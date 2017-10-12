package network

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelFuture
import io.netty.channel.ChannelInitializer
import io.netty.channel.EventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel

import java.net.InetSocketAddress
import java.util.*

/**
 * Created by owen on 17/9/19.
 */
class EchoClient(val sql: String) {
    private val host = "localhost"
    private val port = 12321

    @Throws(InterruptedException::class)
    fun start() {
        val group = NioEventLoopGroup()
        try {
            val b = Bootstrap()
            b.group(group)
                    .channel(NioSocketChannel::class.java)
                    .remoteAddress(InetSocketAddress(host, port))
                    .handler(object : ChannelInitializer<SocketChannel>() {
                        @Throws(Exception::class)
                        override fun initChannel(ch: SocketChannel) {
                            ch.pipeline().addLast(EchoClientHandler(sql))
                        }
                    })

            val f = b.connect().sync()
            f.channel().closeFuture().sync()
        } finally {
            group.shutdownGracefully().sync()
        }
    }



}




fun main(args: Array<String>) {
    while (true){
        val input = Scanner(System.`in`)
        val sql = input.nextLine()
        if (sql == "exit") break
        EchoClient(sql).start()
    }

}

