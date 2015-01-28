package com.mongodb.connection

import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class ServerSettingsSpecification extends Specification {
    def 'should have correct defaults'() {
        when:
        def settings = ServerSettings.builder().build()

        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 10000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 500
    }

    def 'should apply builder settings'() {
        when:
        def settings = ServerSettings.builder()
                                     .heartbeatFrequency(4, SECONDS)
                                     .minHeartbeatFrequency(1, SECONDS)
                                     .build()


        then:
        settings.getHeartbeatFrequency(MILLISECONDS) == 4000
        settings.getMinHeartbeatFrequency(MILLISECONDS) == 1000
    }


    def 'identical settings should be equal'() {
        expect:
        ServerSettings.builder().build() == ServerSettings.builder().build()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .build() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .build()
    }

    def 'different settings should not be equal'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build() != ServerSettings.builder().heartbeatFrequency(3, SECONDS).build()
    }

    def 'identical settings should have same hash code'() {
        expect:
        ServerSettings.builder().build().hashCode() == ServerSettings.builder().build().hashCode()
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .build().hashCode() ==
        ServerSettings.builder()
                      .heartbeatFrequency(4, SECONDS)
                      .minHeartbeatFrequency(1, SECONDS)
                      .build().hashCode()
    }

    def 'different settings should have different hash codes'() {
        expect:
        ServerSettings.builder().heartbeatFrequency(4, SECONDS).build().hashCode() !=
        ServerSettings.builder().heartbeatFrequency(3, SECONDS).build().hashCode()
    }
}