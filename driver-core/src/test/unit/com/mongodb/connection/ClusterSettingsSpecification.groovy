/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection

import com.mongodb.ConnectionString
import com.mongodb.ServerAddress
import com.mongodb.UnixServerAddress
import com.mongodb.event.ClusterListener
import com.mongodb.internal.selector.LatencyMinimizingServerSelector
import com.mongodb.internal.selector.WritableServerSelector
import com.mongodb.selector.CompositeServerSelector
import spock.lang.Specification

import java.util.concurrent.TimeUnit

// TODO: add SRV tests
class ClusterSettingsSpecification extends Specification {
    def hosts = [new ServerAddress('localhost'), new ServerAddress('localhost', 30000)]
    def serverSelector = new WritableServerSelector()
    def defaultServerSelector = new LatencyMinimizingServerSelector(15, TimeUnit.MILLISECONDS)

    def 'should set all default values'() {
        when:
        def settings = ClusterSettings.builder().build()

        then:
        settings.hosts == [new ServerAddress()]
        settings.mode == ClusterConnectionMode.SINGLE
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.serverSelector == defaultServerSelector
        settings.getServerSelectionTimeout(TimeUnit.SECONDS) == 30
        settings.clusterListeners == []
    }

    def 'should set all properties'() {
        given:
        def oneSecondLatencySelector = new LatencyMinimizingServerSelector(1, TimeUnit.SECONDS)
        when:
        def listenerOne = Mock(ClusterListener)
        def listenerTwo = Mock(ClusterListener)
        def settings = ClusterSettings.builder()
                                      .hosts(hosts)
                                      .mode(ClusterConnectionMode.MULTIPLE)
                                      .requiredClusterType(ClusterType.REPLICA_SET)
                                      .requiredReplicaSetName('foo')
                                      .localThreshold(1, TimeUnit.SECONDS)
                                      .serverSelector(serverSelector)
                                      .serverSelectionTimeout(1, TimeUnit.SECONDS)
                                      .addClusterListener(listenerOne)
                                      .addClusterListener(listenerTwo)
                                      .build()

        then:
        settings.hosts == hosts
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.requiredClusterType == ClusterType.REPLICA_SET
        settings.requiredReplicaSetName == 'foo'
        settings.serverSelector == new CompositeServerSelector([serverSelector, oneSecondLatencySelector])
        settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS) == 1000
        settings.clusterListeners == [listenerOne, listenerTwo]
    }

    def 'should apply settings'() {
        given:
        def listenerOne = Mock(ClusterListener)
        def listenerTwo = Mock(ClusterListener)
        def defaultSettings = ClusterSettings.builder().build()
        def customSettings = ClusterSettings.builder()
                .hosts(hosts)
                .mode(ClusterConnectionMode.MULTIPLE)
                .requiredClusterType(ClusterType.REPLICA_SET)
                .requiredReplicaSetName('foo')
                .serverSelector(serverSelector)
                .localThreshold(10, TimeUnit.MILLISECONDS)
                .serverSelectionTimeout(1, TimeUnit.SECONDS)
                .addClusterListener(listenerOne)
                .addClusterListener(listenerTwo)
                .build()

        expect:
        ClusterSettings.builder().applySettings(customSettings).build() == customSettings
        ClusterSettings.builder(customSettings).applySettings(defaultSettings).build() == defaultSettings
    }

    def 'should allow configure serverSelectors correctly'() {
        given:
        def latMinServerSelector = new LatencyMinimizingServerSelector(10, TimeUnit.MILLISECONDS)

        when:
        def settings = ClusterSettings.builder().build()

        then:
        settings.serverSelector == defaultServerSelector

        when:
        settings = ClusterSettings.builder().serverSelector(serverSelector).build()

        then:
        settings.serverSelector == new CompositeServerSelector([serverSelector, defaultServerSelector])

        when:
        settings = ClusterSettings.builder().localThreshold(10, TimeUnit.MILLISECONDS).serverSelector(serverSelector).build()

        then:
        settings.serverSelector == new CompositeServerSelector([serverSelector, latMinServerSelector])
    }

    def 'when connection string is applied to builder, all properties should be set'() {
        when:
        def settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018'))
                                      .build()

        then:
        settings.mode == ClusterConnectionMode.SINGLE
        settings.hosts == [new ServerAddress('example.com:27018')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018,' +
                                                                                        'example.com:27019/?replicaSet=test'))
                                  .build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('example.com:27018'), new ServerAddress('example.com:27019')]
        settings.requiredClusterType == ClusterType.REPLICA_SET
        settings.requiredReplicaSetName == 'test'

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018,example.com:27019'))
                                  .build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('example.com:27018'), new ServerAddress('example.com:27019')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018/?' +
                'serverSelectionTimeoutMS=50000'))
                .build()

        then:
        settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS) == 50000

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://localhost/?localThresholdMS=99')).build()

        then:
        settings.serverSelector == new LatencyMinimizingServerSelector(99, TimeUnit.MILLISECONDS)
    }

    def 'when cluster type is unknown and replica set name is specified, should set cluster type to ReplicaSet'() {
        when:
        def settings = ClusterSettings.builder().hosts([new ServerAddress()]).requiredReplicaSetName('yeah').build()

        then:
        ClusterType.REPLICA_SET == settings.requiredClusterType
    }

    def 'connection mode should default to single if one host or multiple if more'() {
        when:
        def settings = ClusterSettings.builder().hosts([new ServerAddress()]).build()

        then:
        settings.mode == ClusterConnectionMode.SINGLE

        when:
        settings = ClusterSettings.builder().hosts(hosts).build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
    }

    def 'when mode is Single and hosts size is greater than one, should throw'() {
        when:
        ClusterSettings.builder().hosts([new ServerAddress(), new ServerAddress('other')]).mode(ClusterConnectionMode.SINGLE).build()
        then:
        thrown(IllegalArgumentException)
    }

    def 'when cluster type is Standalone and multiple hosts are specified, should throw'() {
        when:
        ClusterSettings.builder().hosts([new ServerAddress(), new ServerAddress('other')]).requiredClusterType(ClusterType.STANDALONE)
                       .build()
        then:
        thrown(IllegalArgumentException)
    }

    def 'when a replica set name is specified and type is Standalone, should throw'() {
        when:
        ClusterSettings.builder().hosts([new ServerAddress(), new ServerAddress('other')]).requiredReplicaSetName('foo')
                       .requiredClusterType(ClusterType.STANDALONE).build()
        then:
        thrown(IllegalArgumentException)
    }

    def 'when a replica set name is specified and type is Sharded, should throw'() {
        when:
        ClusterSettings.builder().hosts([new ServerAddress(), new ServerAddress('other')]).requiredReplicaSetName('foo')
                       .requiredClusterType(ClusterType.SHARDED).build()
        then:
        thrown(IllegalArgumentException)
    }


    def 'should throws if hosts list is null'() {
        when:
        ClusterSettings.builder().hosts(null).build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throws if hosts list is empty'() {
        when:
        ClusterSettings.builder().hosts([]).build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throws if hosts list contains null value'() {
        when:
        ClusterSettings.builder().hosts([null]).build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'should remove duplicate hosts'() {
        when:
        def settings = ClusterSettings.builder().hosts([new ServerAddress('server1'),
                                                        new ServerAddress('server2'),
                                                        new ServerAddress('server1')]).build()

        then:
        settings.getHosts() == [new ServerAddress('server1'), new ServerAddress('server2')]
    }

    def 'identical settings should be equal'() {
        expect:
        ClusterSettings.builder().hosts(hosts).build() == ClusterSettings.builder().hosts(hosts).build()
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build() ==
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build()
    }

    def 'different settings should not be equal'() {
        expect:
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build() != ClusterSettings.builder().hosts(hosts).build()
    }

    def 'identical settings should have same hash code'() {
        expect:
        ClusterSettings.builder().hosts(hosts).build().hashCode() == ClusterSettings.builder().hosts(hosts).build().hashCode()
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build().hashCode() ==
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build().hashCode()
    }

    def 'different settings should have different hash codes'() {
        expect:
        ClusterSettings.builder()
                       .hosts(hosts)
                       .mode(ClusterConnectionMode.MULTIPLE)
                       .requiredClusterType(ClusterType.REPLICA_SET)
                       .requiredReplicaSetName('foo')
                       .serverSelector(serverSelector)
                       .serverSelectionTimeout(1, TimeUnit.SECONDS)
                       .build().hashCode() != ClusterSettings.builder().hosts(hosts).build().hashCode()
    }

    def 'should replace unknown ServerAddress subclass instances with ServerAddress'() {
        when:
        def settings = ClusterSettings.builder().hosts([new ServerAddress('server1'),
                                                        new ServerAddressSubclass('server2'),
                                                        new UnixServerAddress('mongodb.sock')]).build()

        then:
        settings.getHosts() == [new ServerAddress('server1'), new ServerAddress('server2'), new UnixServerAddress('mongodb.sock')]
    }

    def 'list of cluster listeners should be unmodifiable'() {
        given:
        def settings = ClusterSettings.builder().hosts(hosts).build()

        when:
        settings.clusterListeners.add(Mock(ClusterListener))

        then:
        thrown(UnsupportedOperationException)
    }

    def 'cluster listener should not be null'() {
       when:
       ClusterSettings.builder().addClusterListener(null)

       then:
       thrown(IllegalArgumentException)
    }

    static class ServerAddressSubclass extends ServerAddress {
        ServerAddressSubclass(final String host) {
            super(host)
        }
    }
}
