package com.mongodb.internal.connection

import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.AsyncCompletionHandler
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.spi.dns.InetAddressResolver
import spock.lang.IgnoreIf
import spock.lang.Specification
import util.spock.annotations.Slow

import java.util.concurrent.CountDownLatch

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getSslSettings
import static java.util.concurrent.TimeUnit.MILLISECONDS

class AsyncSocketChannelStreamSpecification extends Specification {

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should successfully connect with working ip address list'() {
        given:
        def socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()

        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                [InetAddress.getByName('192.168.255.255'),
                 InetAddress.getByName('127.0.0.1')]
            }
        }

        def factoryFactory = new AsynchronousSocketChannelStreamFactoryFactory(inetAddressResolver)
        def factory = factoryFactory.create(socketSettings, sslSettings)

        def stream = factory.create(new ServerAddress('host1'))

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        !stream.isClosed()
    }

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should fail to connect with non-working ip address list'() {
        given:
        def socketSettings = SocketSettings.builder().connectTimeout(100, MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()

        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                [InetAddress.getByName('192.168.255.255'),
                 InetAddress.getByName('1.2.3.4')]
            }
        }

        def factoryFactory = new AsynchronousSocketChannelStreamFactoryFactory(inetAddressResolver)
        def factory = factoryFactory.create(socketSettings, sslSettings)
        def stream = factory.create(new ServerAddress())

        when:
        stream.open(OPERATION_CONTEXT)

        then:
        thrown(MongoSocketOpenException)
    }

    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should fail AsyncCompletionHandler if name resolution fails'() {
        given:
        def serverAddress = new ServerAddress()
        def exception = new MongoSocketException('Temporary failure in name resolution', serverAddress)

        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                throw exception
            }
        }
        def stream = new AsynchronousSocketChannelStream(serverAddress, inetAddressResolver,
                SocketSettings.builder().connectTimeout(100, MILLISECONDS).build(),
                new PowerOfTwoBufferPool())
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
