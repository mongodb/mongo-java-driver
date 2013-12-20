package org.mongodb

import org.mongodb.connection.ServerAddress
import spock.lang.Specification
import spock.lang.Unroll


class ServerAddressSpecification extends Specification {

    @Unroll
    def "constructors should parse hostname and port correctly"() {
        expect:
        address.getHost() == host;
        address.getPort() == port;

        where:
        address                                                 | host                          | port
        new ServerAddress()                                     | ServerAddress.getDefaultHost()| ServerAddress.getDefaultPort()
        new ServerAddress("10.0.0.1")                           | "10.0.0.1"                    | ServerAddress.getDefaultPort()
        new ServerAddress("10.0.0.1:1000")                      | "10.0.0.1"                    | 1000
        new ServerAddress("10.0.0.1", 1000)                     | "10.0.0.1"                    | 1000
        new ServerAddress("somewhere")                          | "somewhere"                   | ServerAddress.getDefaultPort()
        new ServerAddress("somewhere:1000")                     | "somewhere"                   | 1000
        new ServerAddress("somewhere", 1000)                    | "somewhere"                   | 1000
    }

    def "ipv4 host with a port specified should throw when a port is also specified as an argument"() {
        when:
        new ServerAddress("10.0.0.1:80", 80)
        then:
        thrown(IllegalArgumentException);

        when:
        new ServerAddress("10.0.0.1:1000", 80)
        then:
        thrown(IllegalArgumentException);
    }
    
    def "hostname with a port specified should throw when a port is also specified as an argument"() {
        when:
        new ServerAddress("somewhere:80", 80)
        then:
        thrown(IllegalArgumentException);

        when:
        new ServerAddress("somewhere:1000", 80);
        then:
        thrown(IllegalArgumentException);
    }
}