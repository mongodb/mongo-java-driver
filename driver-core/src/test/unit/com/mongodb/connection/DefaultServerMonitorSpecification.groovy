/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.ServerAddress
import spock.lang.Specification

import java.util.concurrent.CountDownLatch

class DefaultServerMonitorSpecification extends Specification {

    DefaultServerMonitor monitor

    def 'A thread interrupt should send a sendStateChangedEvent'() {
        given:
        def stateChanged = false
        def latch = new CountDownLatch(1);
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
                latch.countDown()
            }
        }
        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open() >> { throw new IOException() }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerAddress(), ServerSettings.builder().build(), 'clusterId', changeListener,
                                           internalConnectionFactory, new TestConnectionPool())
        monitor.start()

        when:
        monitor.monitorThread.interrupt()
        latch.await()

        then:
        stateChanged

        cleanup:
        monitor?.close()
    }

    def 'invalidate should not send a sendStateChangedEvent'() {
        given:
        def stateChanged = false
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
            }
        }
        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open() >> { sleep(10); }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerAddress(), ServerSettings.builder().build(), 'clusterId', changeListener,
                                           internalConnectionFactory, new TestConnectionPool())
        monitor.start()
        def monitorId = monitor.monitorThread.getId()

        when:
        monitor.invalidate()
        while (monitorId == monitor.monitorThread.getId()) { monitor.monitorThread.wait(10) }

        then:
        !stateChanged

        cleanup:
        monitor?.close()
    }

    def 'close should not send a sendStateChangedEvent'() {
        given:
        def stateChanged = false
        def changeListener = new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
            }
        }
        def internalConnectionFactory = Mock(InternalConnectionFactory) {
            create(_) >> {
                Mock(InternalConnection) {
                    open() >> { sleep(10); }
                }
            }
        }
        monitor = new DefaultServerMonitor(new ServerAddress(), ServerSettings.builder().build(), 'clusterId', changeListener,
                                           internalConnectionFactory, new TestConnectionPool())
        monitor.start()

        when:
        monitor.close()

        then:
        monitor.monitorThread.isInterrupted()
        !stateChanged
    }

}
