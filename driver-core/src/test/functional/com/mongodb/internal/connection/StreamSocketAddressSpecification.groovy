package com.mongodb.internal.connection

import com.mongodb.MongoSocketOpenException
import com.mongodb.ServerAddress
import com.mongodb.connection.SocketSettings
import com.mongodb.connection.SslSettings
import com.mongodb.spi.dns.InetAddressResolver
import spock.lang.Ignore
import spock.lang.IgnoreIf
import spock.lang.Specification
import com.mongodb.spock.Slow

import javax.net.SocketFactory
import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.ClusterFixture.getSslSettings

class StreamSocketAddressSpecification extends Specification {

    @Slow
    @Ignore
    def 'should successfully connect with working ip address group'() {
        given:
        def port = 27017
        def socketSettings = SocketSettings.builder().connectTimeout(100, TimeUnit.MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()
        def bufferProvider = Stub(BufferProvider)

        def inetAddresses = new InetSocketAddress[3]
        inetAddresses[0] = new InetSocketAddress(InetAddress.getByName('1.2.3.4'), port)
        inetAddresses[1] = new InetSocketAddress(InetAddress.getByName('2.3.4.5'), port)
        inetAddresses[2] = new InetSocketAddress(InetAddress.getByName('127.0.0.1'), port)

        def serverAddress = Stub(ServerAddress)
        serverAddress.getSocketAddresses() >> inetAddresses

        def socketFactory = Stub(SocketFactory)
        def socket0 = SocketFactory.default.createSocket()
        def socket1 = SocketFactory.default.createSocket()
        def socket2 = SocketFactory.default.createSocket()
        socketFactory.createSocket() >>> [socket0, socket1, socket2]

        def socketStream = new SocketStream(serverAddress, null, socketSettings, sslSettings, socketFactory, bufferProvider)

        when:
        socketStream.open(OPERATION_CONTEXT)

        then:
        !socket0.isConnected()
        !socket1.isConnected()
        socket2.isConnected()

        cleanup:
        socketStream?.close()
    }

    @Slow
    @IgnoreIf({ getSslSettings().isEnabled() })
    def 'should throw exception when attempting to connect with incorrect ip address group'() {
        given:
        def socketSettings = SocketSettings.builder().connectTimeout(100, TimeUnit.MILLISECONDS).build()
        def sslSettings = SslSettings.builder().build()
        def bufferProvider = Stub(BufferProvider)

        def serverAddress = new ServerAddress()

        def socketFactory = Stub(SocketFactory)
        def socket0 = SocketFactory.default.createSocket()
        def socket1 = SocketFactory.default.createSocket()
        def socket2 = SocketFactory.default.createSocket()
        socketFactory.createSocket() >>> [socket0, socket1, socket2]

        def inetAddressResolver = new InetAddressResolver() {
            @Override
            List<InetAddress> lookupByName(String host) {
                [InetAddress.getByName('1.2.3.4'),
                 InetAddress.getByName('2.3.4.5'),
                 InetAddress.getByName('1.2.3.5')]
            }
        }

        def socketStream = new SocketStream(serverAddress, inetAddressResolver, socketSettings, sslSettings, socketFactory, bufferProvider)

        when:
        socketStream.open(OPERATION_CONTEXT)

        then:
        thrown(MongoSocketOpenException)
        !socket0.isConnected()
        !socket1.isConnected()
        !socket2.isConnected()

        cleanup:
        socketStream?.close()
    }
}
