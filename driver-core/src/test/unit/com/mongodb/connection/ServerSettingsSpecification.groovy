package com.mongodb.connection

import com.mongodb.ConnectionString
import com.mongodb.event.ServerListenerAdapter
import com.mongodb.event.ServerMonitorListenerAdapter
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

@SuppressWarnings('deprecation')
class ServerSettingsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def settings = ServerSettings.builder().build()

        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 10000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 500
        settings.serverListeners == []
        settings.serverMonitorListeners == []
    }

    def 'should apply builder settings'() {
        given:
        def serverListenerOne = new ServerListenerAdapter() { }
        def serverListenerTwo = new ServerListenerAdapter() { }
        def serverMonitorListenerOne = new ServerMonitorListenerAdapter() { }
        def serverMonitorListenerTwo = new ServerMonitorListenerAdapter() { }

        when:
        def settings = ServerSettings.builder()
                .heartbeatFrequency(4, SECONDS)
                .minHeartbeatFrequency(1, SECONDS)
                .addServerListener(serverListenerOne)
                .addServerListener(serverListenerTwo)
                .addServerMonitorListener(serverMonitorListenerOne)
                .addServerMonitorListener(serverMonitorListenerTwo)
                .build()


        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 4000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 1000
        settings.serverListeners == [serverListenerOne, serverListenerTwo]
        settings.serverMonitorListeners == [serverMonitorListenerOne, serverMonitorListenerTwo]
    }

    def 'when connection string is applied to builder, all properties should be set'() {
        when:
        def settings = ServerSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018/?' +
                'heartbeatFrequencyMS=20000'))
                .build()

        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 20000
    }

    def 'lists of listeners should be unmodifiable'() {
        given:
        def settings = ServerSettings.builder().build()

        when:
        settings.serverListeners.add(new ServerListenerAdapter() { })

        then:
        thrown(UnsupportedOperationException)

        when:
        settings.serverMonitorListeners.add(new ServerMonitorListenerAdapter() { })

        then:
        thrown(UnsupportedOperationException)
    }

    def 'listeners should not be null'() {
        when:
        ServerSettings.builder().addServerListener(null)

        then:
        thrown(IllegalArgumentException)

        when:
        ServerSettings.builder().addServerMonitorListener(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'identical settings should be equal'() {
        given:
        def serverListenerOne = new ServerListenerAdapter() { }
        def serverMonitorListenerOne = new ServerMonitorListenerAdapter() { }

        expect:
        ServerSettings.builder().build() == ServerSettings.builder().build()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build()
    }

    def 'different settings should not be equal'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build() != ServerSettings.builder().heartbeatFrequency(3, SECONDS).build()
    }

    def 'identical settings should have same hash code'() {
        given:
        def serverListenerOne = new ServerListenerAdapter() { }
        def serverMonitorListenerOne = new ServerMonitorListenerAdapter() { }

        expect:
        ServerSettings.builder().build().hashCode() == ServerSettings.builder().build().hashCode()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build().hashCode() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .addServerListener(serverListenerOne)
                      .addServerMonitorListener(serverMonitorListenerOne)
                      .build().hashCode()
    }

    def 'different settings should have different hash codes'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build().hashCode() !=
        ServerSettings.builder().heartbeatFrequency(3, SECONDS).build().hashCode()
    }
}
