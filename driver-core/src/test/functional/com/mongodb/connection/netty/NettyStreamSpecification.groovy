package com.mongodb.connection.netty

import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.AsyncCompletionHandler
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.internal.connection.netty.NettyStreamFactory
import com.mongodb.spi.dns.InetAddressResolver
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioSocketChannel
import spock.lang.IgnoreIf
import spock.lang.Specification
import util.spock.annotations.Slow

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getSslSettings

class NettyStreamSpecification extends Specification {

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should successfully connect with working ip address group'() {
        given:
        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
        SslSettings sslSettings = SslSettings.builder().build()
        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                [InetAddress.getByName('192.168.255.255'),
                 InetAddress.getByName('1.2.3.4'),
                 InetAddress.getByName('127.0.0.1')]
            }
        }
        def factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings, new NioEventLoopGroup(),
                NioSocketChannel, PooledByteBufAllocator.DEFAULT, null)

        def stream = factory.create(new ServerAddress())

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        !stream.isClosed()
    }

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should throw exception with non-working ip address group'() {
        given:
        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
        SslSettings sslSettings = SslSettings.builder().build()
        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                [InetAddress.getByName('192.168.255.255'),
                 InetAddress.getByName('1.2.3.4'),
                 InetAddress.getByName('1.2.3.5')]
            }
        }
        def factory = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings, new NioEventLoopGroup(),
                NioSocketChannel, PooledByteBufAllocator.DEFAULT, null)

        def stream = factory.create(new ServerAddress())

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        thrown(MongoSocketOpenException)
    }

    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should fail AsyncCompletionHandler if name resolution fails'() {
        given:
        def serverAddress = Stub(ServerAddress)
        def exception = new MongoSocketException('Temporary failure in name resolution', serverAddress)
        serverAddress.getSocketAddresses() >> { throw exception }

        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
        SslSettings sslSettings = SslSettings.builder().build()
        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                throw exception
            }
        }
        def stream = new NettyStreamFactory(inetAddressResolver, socketSettings, sslSettings, new NioEventLoopGroup(),
                NioSocketChannel, PooledByteBufAllocator.DEFAULT, null)
                .create(new ServerAddress())
        def callback = new CallbackErrorHolder()

        when:
        stream.openAsync(OPERATION_CONTEXT, callback)

        then:
        callback.getError().is(exception)
    }

    class CallbackErrorHolder implements AsyncCompletionHandler<Void> {
        CountDownLatch latch = new CountDownLatch(1)
        Throwable throwable = null

        Throwable getError() {
            latch.countDown()
            throwable
        }

        @Override
        void completed(Void r) {
            latch.await()
        }

        @Override
        void failed(Throwable t) {
            throwable = t
            latch.countDown()
        }
    }
}
