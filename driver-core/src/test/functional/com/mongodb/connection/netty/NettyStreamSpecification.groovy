package com.mongodb.connection.netty

import category.Slow
import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.getSslSettings

class NettyStreamSpecification extends Specification {

    @Category(Slow)
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should successfully connect with working ip address group'() {
        given:
        def port = 27017
        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
        SslSettings sslSettings = SslSettings.builder().build()
        def factory = new NettyStreamFactory(socketSettings, sslSettings)

        def inetAddresses = [new InetSocketAddress(InetAddress.getByName('192.168.255.255'), port),
                             new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port),
                             new InetSocketAddress(InetAddress.getByName('127.0.0.1'), port)]

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def stream = factory.create(serverAddress)

        when:
        stream.open()

        then:
        !stream.isClosed()
    }

    @Category(Slow)
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should throw exception with non-working ip address group'() {
        given:
        def port = 27017
        SocketSettings socketSettings = SocketSettings.builder().connectTimeout(1000, TimeUnit.MILLISECONDS).build()
        SslSettings sslSettings = SslSettings.builder().build()
        def factory = new NettyStreamFactory(socketSettings, sslSettings)

        def inetAddresses = [new InetSocketAddress(InetAddress.getByName('192.168.255.255'), port),
                             new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port),
                             new InetSocketAddress(InetAddress.getByName('1.2.3.5'), port)]

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def stream = factory.create(serverAddress)

        when:
        stream.open()

        then:
        thrown(MongoSocketOpenException)
    }
}
