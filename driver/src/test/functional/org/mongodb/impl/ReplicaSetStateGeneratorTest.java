/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.MongoException;
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.io.MongoSocketReadTimeoutException;
import org.mongodb.result.CommandResult;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;
import org.mongodb.rs.Tag;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReplicaSetStateGeneratorTest {
    private ServerAddress serverAddress;
    private Document isMasterCommand = new Document("ismaster", 1);
    private Document isMasterResponse;
    private Document errorIsMasterResponse;
    private Map<ServerAddress, CommandResult> commandResultMap;
    private Map<ServerAddress, MongoException> exceptionMap;
    private MockIsMasterExecutorFactory masterExecutorFactory = MockIsMasterExecutorFactory.withCommandResultMap(commandResultMap);

    @Before
    public void before() throws UnknownHostException {
        serverAddress = new ServerAddress();
        isMasterResponse = new Document("ok", 1.0)
                .append("ismaster", true)
                .append("secondary", false)
                .append("maxBsonObjectSize", 4 * 1024 * 1024)
                .append("setName", "test");
        errorIsMasterResponse = new Document("ok", 0.0);
        commandResultMap = new HashMap<ServerAddress, CommandResult>();
        exceptionMap = new HashMap<ServerAddress, MongoException>();
        masterExecutorFactory = new MockIsMasterExecutorFactory(commandResultMap, exceptionMap);
    }

    @Test
    public void shouldReturnReplicaSetMemberThatIsNotOkIfCommandResultIsNotOk() throws UnknownHostException {
        commandResultMap.put(serverAddress, new CommandResult(isMasterCommand, serverAddress, errorIsMasterResponse, 35000004));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress), masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(new ReplicaSetMember(serverAddress), replicaSetState.getMember(serverAddress));
    }

    @Test
    public void shouldReturnReplicaSetMemberThatIsNotOkIfCommandThrowsException() {
        exceptionMap.put(serverAddress, new MongoSocketReadTimeoutException("", serverAddress, new SocketTimeoutException()));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress),
                        masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(new ReplicaSetMember(serverAddress), replicaSetState.getMember(serverAddress));
    }

    @Test
    public void shouldCreateCorrectReplicaSetMember() {
        commandResultMap.put(serverAddress, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 35000004));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress),
                        masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(new ReplicaSetMember(serverAddress, "test", 35.000004f, true, true, false, new HashSet<Tag>(), 4 * 1024 * 1024),
                replicaSetState.getMember(serverAddress));
    }

    @Test
    public void shouldCreateCorrectReplicaSetMemberWithTags() {
        isMasterResponse.append("tags", new Document("dc", "ny").append("rack", "1"));
        commandResultMap.put(serverAddress, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 35000004));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress),
                        masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(new HashSet<Tag>(Arrays.asList(new Tag("dc", "ny"), new Tag("rack", "1"))),
                replicaSetState.getMember(serverAddress).getTags());
    }

    @Test
    public void shouldAddChannelStatesForHostsAndPassives() throws UnknownHostException {
        ServerAddress serverAddress2 = new ServerAddress("localhost:27018");
        ServerAddress serverAddress3 = new ServerAddress("localhost:27019");

        isMasterResponse.append("hosts", Arrays.asList("localhost:27017", "localhost:27018"))
                .append("passives", Arrays.asList("localhost:27019"));
        commandResultMap.put(serverAddress, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 35000004));
        commandResultMap.put(serverAddress2, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 35000004));
        commandResultMap.put(serverAddress3, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 35000004));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress),
                        masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(3, replicaSetState.getAll().size());
        assertEquals(3, generator.getChannelStates().size());
        assertNotNull(replicaSetState.getMember(serverAddress));
        assertTrue(replicaSetState.getMember(serverAddress).isOk());
        assertNotNull(replicaSetState.getMember(serverAddress2));
        assertTrue(replicaSetState.getMember(serverAddress2).isOk());
        assertNotNull(replicaSetState.getMember(serverAddress3));
        assertTrue(replicaSetState.getMember(serverAddress3).isOk());
        assertTrue(generator.getChannelStates().contains(
                new ReplicaSetMonitor.ReplicaSetStateGenerator.ChannelState(new MockIsMasterExecutor(serverAddress))));
        assertTrue(generator.getChannelStates().contains(
                new ReplicaSetMonitor.ReplicaSetStateGenerator.ChannelState(new MockIsMasterExecutor(serverAddress2))));
        assertTrue(generator.getChannelStates().contains(
                new ReplicaSetMonitor.ReplicaSetStateGenerator.ChannelState(new MockIsMasterExecutor(serverAddress3))));
    }

    @Test
    public void testPingTimeSmoothing() {
        commandResultMap.put(serverAddress, new CommandResult(isMasterCommand, serverAddress, isMasterResponse, 3500004));
        ReplicaSetMonitor.ReplicaSetStateGenerator generator =
                new ReplicaSetMonitor.ReplicaSetStateGenerator(Arrays.asList(serverAddress),
                        masterExecutorFactory, 4.0f);
        ReplicaSet replicaSetState = generator.getReplicaSetState();
        assertEquals(3.5000040531158447f, replicaSetState.getMember(serverAddress).getPingTime(), 0.0);

        masterExecutorFactory.all.get(serverAddress).commandResult = new CommandResult(isMasterCommand, serverAddress,
                isMasterResponse, 2500004);
        replicaSetState = generator.getReplicaSetState();
        assertEquals(3.2500040531158447f, replicaSetState.getMember(serverAddress).getPingTime(), 0.0);

        masterExecutorFactory.all.get(serverAddress).commandResult = new CommandResult(isMasterCommand, serverAddress,
                isMasterResponse, 500004);
        replicaSetState = generator.getReplicaSetState();
        assertEquals(2.5625040531158447f, replicaSetState.getMember(serverAddress).getPingTime(), 0.0);
    }


    static class MockIsMasterExecutorFactory implements AbstractConnectionSetMonitor.IsMasterExecutorFactory {
        private final Map<ServerAddress, CommandResult> commandResultMap;
        private final Map<ServerAddress, MongoException> exceptionMap;
        private final Map<ServerAddress, MockIsMasterExecutor> all = new HashMap<ServerAddress, MockIsMasterExecutor>();

        public MockIsMasterExecutorFactory(final Map<ServerAddress, CommandResult> commandResultMap,
                                           final Map<ServerAddress, MongoException> exceptionMap) {
            this.commandResultMap = commandResultMap;
            this.exceptionMap = exceptionMap;
        }

        public static MockIsMasterExecutorFactory withCommandResultMap(final Map<ServerAddress, CommandResult> commandResultMap) {
            final Map<ServerAddress, MongoException> exceptionMap = Collections.emptyMap();
            return new MockIsMasterExecutorFactory(commandResultMap, exceptionMap);
        }

        public static MockIsMasterExecutorFactory withExceptionMap(final Map<ServerAddress, MongoException> exceptionMap) {
            final Map<ServerAddress, CommandResult> commandResultMap = Collections.emptyMap();
            return new MockIsMasterExecutorFactory(commandResultMap, exceptionMap);
        }

        @Override
        public AbstractConnectionSetMonitor.IsMasterExecutor create(final ServerAddress serverAddress) {
            MockIsMasterExecutor retVal;
            if (exceptionMap.get(serverAddress) != null) {
                retVal = new MockIsMasterExecutor(serverAddress, exceptionMap.get(serverAddress));
            } else {
                retVal = new MockIsMasterExecutor(serverAddress, commandResultMap.get(serverAddress));
            }
            all.put(serverAddress, retVal);
            return retVal;
        }
    }

    static class MockIsMasterExecutor implements AbstractConnectionSetMonitor.IsMasterExecutor {

        private final ServerAddress serverAddress;
        private CommandResult commandResult;
        private MongoException exception;

        MockIsMasterExecutor(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
            this.exception = null;
            this.commandResult = null;
        }

        MockIsMasterExecutor(final ServerAddress serverAddress, final CommandResult commandResult) {
            this.serverAddress = serverAddress;
            this.commandResult = commandResult;
            this.exception = null;
        }

        MockIsMasterExecutor(final ServerAddress serverAddress, final MongoException exception) {
            this.serverAddress = serverAddress;
            this.exception = exception;
            this.commandResult = null;
        }

        @Override
        public IsMasterCommandResult execute() {
            if (exception != null) {
                throw exception;
            }

            return new IsMasterCommandResult(commandResult);
        }

        @Override
        public ServerAddress getServerAddress() {
            return serverAddress;
        }

        @Override
        public void close() {
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MockIsMasterExecutor that = (MockIsMasterExecutor) o;

            if (!serverAddress.equals(that.serverAddress)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            return serverAddress.hashCode();
        }
    }
}
