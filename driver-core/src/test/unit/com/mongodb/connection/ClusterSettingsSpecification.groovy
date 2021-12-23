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
import com.mongodb.internal.selector.WritableServerSelector
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class ClusterSettingsSpecification extends Specification {
    def hosts = [new ServerAddress('localhost'), new ServerAddress('localhost', 30000)]
    def serverSelector = new WritableServerSelector()

    def 'should set all default values'() {
        when:
        def settings = ClusterSettings.builder().build()

        then:
        settings.hosts == [new ServerAddress()]
        settings.mode == ClusterConnectionMode.SINGLE
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.serverSelector == null
        settings.getServerSelectionTimeout(TimeUnit.SECONDS) == 30
        settings.clusterListeners == []
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'
    }

    def 'should set all properties'() {
        when:
        def listenerOne = Mock(ClusterListener)
        def listenerTwo = Mock(ClusterListener)
        def listenerThree = Mock(ClusterListener)
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
        settings.serverSelector == serverSelector
        settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS) == 1000
        settings.clusterListeners == [listenerOne, listenerTwo]

        when:
        settings = ClusterSettings.builder(settings).clusterListenerList([listenerThree]).build()

        then:
        settings.clusterListeners == [listenerThree]
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

    def 'should apply settings for SRV'() {
        given:
        def defaultSettings = ClusterSettings.builder().build()
        def customSettings = ClusterSettings.builder()
                .hosts([new ServerAddress('localhost')])
                .srvMaxHosts(4)
                .srvServiceName('foo')
                .build()

        expect:
        ClusterSettings.builder().applySettings(customSettings).build() == customSettings
        ClusterSettings.builder(customSettings).applySettings(defaultSettings).build() == defaultSettings
    }

    def 'when hosts contains more than one element and mode is SINGLE, should throw IllegalArgumentException'() {
        when:
        def builder = ClusterSettings.builder()
        builder.hosts([new ServerAddress('host1'), new ServerAddress('host2')])
        builder.mode(ClusterConnectionMode.SINGLE)
        builder.build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'when hosts contains more than one element and mode is LOAD_BALANCED, should throw IllegalArgumentException'() {
        when:
        def builder = ClusterSettings.builder()
        builder.hosts([new ServerAddress('host1'), new ServerAddress('host2')])
        builder.mode(ClusterConnectionMode.LOAD_BALANCED)
        builder.build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'when srvHost is specified, should set mode to MULTIPLE if mode is not configured'() {
        when:
        def builder = ClusterSettings.builder()
        builder.srvHost('foo.bar.com')
        def settings = builder.build()

        then:
        settings.getSrvHost() == 'foo.bar.com'
        settings.getMode() == ClusterConnectionMode.MULTIPLE
    }

    def 'when srvHost is specified, should use configured mode is load balanced'() {
        when:
        def builder = ClusterSettings.builder()
        builder.srvHost('foo.bar.com')
        builder.mode(ClusterConnectionMode.LOAD_BALANCED)
        def settings = builder.build()

        then:
        settings.getSrvHost() == 'foo.bar.com'
        settings.getMode() == ClusterConnectionMode.LOAD_BALANCED
    }

    def 'when srvHost contains a colon, should throw IllegalArgumentException'() {
        when:
        def builder = ClusterSettings.builder()
        builder.srvHost('foo.bar.com:27017')
        builder.build()

        then:
        thrown(IllegalArgumentException)
    }

    def 'when srvHost contains less than three parts (host, domain, top-level domain, should throw IllegalArgumentException'() {
        when:
        def builder = ClusterSettings.builder()
        builder.srvHost('foo.bar')
        builder.build()

        then:
        thrown(IllegalArgumentException)
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
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb+srv://test5.test.build.10gen.cc/')).build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('127.0.0.1:27017')]
        settings.requiredClusterType == ClusterType.REPLICA_SET
        settings.requiredReplicaSetName == 'repl0'
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder().applyConnectionString(
                new ConnectionString('mongodb+srv://test22.test.build.10gen.cc/?srvServiceName=customname&srvMaxHosts=1')).build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('127.0.0.1:27017')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.srvMaxHosts == 1
        settings.srvServiceName == 'customname'

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018/?replicaSet=test'))
                                  .build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('example.com:27018')]
        settings.requiredClusterType == ClusterType.REPLICA_SET
        settings.requiredReplicaSetName == 'test'
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://example.com:27018/?directConnection=false'))
                .build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('example.com:27018')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://example.com:27018/?directConnection=true'))
                .build()

        then:
        settings.mode == ClusterConnectionMode.SINGLE
        settings.hosts == [new ServerAddress('example.com:27018')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018,example.com:27019'))
                                  .build()

        then:
        settings.mode == ClusterConnectionMode.MULTIPLE
        settings.hosts == [new ServerAddress('example.com:27018'), new ServerAddress('example.com:27019')]
        settings.requiredClusterType == ClusterType.UNKNOWN
        settings.requiredReplicaSetName == null
        settings.srvMaxHosts == null
        settings.srvServiceName == 'mongodb'

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://example.com:27018/?' +
                'serverSelectionTimeoutMS=50000'))
                .build()

        then:
        settings.getServerSelectionTimeout(TimeUnit.MILLISECONDS) == 50000

        when:
        settings = ClusterSettings.builder().applyConnectionString(new ConnectionString('mongodb://localhost/?localThresholdMS=99')).build()

        then:
        settings.getLocalThreshold(TimeUnit.MILLISECONDS) == 99

        when:
        settings = ClusterSettings.builder()
                .applyConnectionString(new ConnectionString('mongodb://example.com:27018/?loadBalanced=true')).build()

        then:
        settings.mode == ClusterConnectionMode.LOAD_BALANCED
        settings.hosts == [new ServerAddress('example.com:27018')]
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
