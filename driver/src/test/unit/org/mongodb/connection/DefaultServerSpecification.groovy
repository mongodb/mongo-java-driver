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







package org.mongodb.connection

import spock.lang.Specification

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService

class DefaultServerSpecification extends Specification {

    static ScheduledExecutorService scheduledExecutorService
    DefaultServer server;

    def setupSpec() {
        scheduledExecutorService = Executors.newScheduledThreadPool(1)
    }

    def cleanupSpec() {
        scheduledExecutorService.shutdown()
    }

    def setup() {
        server = new DefaultServer(new ServerAddress(), ServerSettings.builder().build(), 'cluster-1', new TestConnectionPool(),
                                   new TestInternalConnectionFactory())
    }

    def cleanup() {
        server.close()
    }

    def 'invalidate should invoke change listeners'() {
        given:
        def stateChanged = false;

        server.addChangeListener(new ChangeListener<ServerDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ServerDescription> event) {
                stateChanged = true;
            }
        })

        when:
        server.invalidate();

        then:
        stateChanged
    }
}