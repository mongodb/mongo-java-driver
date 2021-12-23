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

package com.mongodb.internal.connection;

import com.mongodb.MongoClientException;
import com.mongodb.MongoConfigurationException;
import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.ServerType;
import com.mongodb.selector.ServerSelector;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadBalancedClusterTest {
    private LoadBalancedCluster cluster;

    @BeforeEach
    public void after() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    public void shouldSelectServerWhenThereIsNoSRVLookup() {
        // given
        ServerAddress serverAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .hosts(Collections.singletonList(serverAddress))
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(serverAddress, expectedServer);
        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, mock(DnsSrvRecordMonitorFactory.class));

        // when
        ServerTuple serverTuple = cluster.selectServer(mock(ServerSelector.class));

        // then
        assertServerTupleExpectations(serverAddress, expectedServer, serverTuple);

        // when
        FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
        cluster.selectServerAsync(mock(ServerSelector.class), callback);
        serverTuple = callback.get();

        // then
        assertServerTupleExpectations(serverAddress, expectedServer, serverTuple);
    }

    @Test
    public void shouldSelectServerWhenThereIsSRVLookup() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2)));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        // when
        ServerTuple serverTuple = cluster.selectServer(mock(ServerSelector.class));

        // then
        assertServerTupleExpectations(resolvedServerAddress, expectedServer, serverTuple);
    }

    @Test
    public void shouldSelectServerAsynchronouslyWhenThereIsSRVLookup() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2)));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        // when
        FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
        cluster.selectServerAsync(mock(ServerSelector.class), callback);
        ServerTuple serverTuple = callback.get();

        // then
        assertServerTupleExpectations(resolvedServerAddress, expectedServer, serverTuple);
    }

    @Test
    public void shouldFailSelectServerWhenThereIsSRVMisconfiguration() {
        // given
        String srvHostName = "foo.bar.com";
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory();

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2))
                        .hosts(Arrays.asList(new ServerAddress("host1"), new ServerAddress("host2"))));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        MongoClientException exception = assertThrows(MongoClientException.class, () -> cluster.selectServer(mock(ServerSelector.class)));
        assertEquals("In load balancing mode, the host must resolve to a single SRV record, but instead it resolved to multiple hosts",
                exception.getMessage());
    }

    @Test
    public void shouldFailSelectServerAsynchronouslyWhenThereIsSRVMisconfiguration() {
        // given
        String srvHostName = "foo.bar.com";
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory();

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2))
                        .hosts(Arrays.asList(new ServerAddress("host1"), new ServerAddress("host2"))));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
        cluster.selectServerAsync(mock(ServerSelector.class), callback);

        MongoClientException exception = assertThrows(MongoClientException.class, callback::get);
        assertEquals("In load balancing mode, the host must resolve to a single SRV record, but instead it resolved to multiple hosts",
                exception.getMessage());
    }

    @Test
    public void shouldTimeoutSelectServerWhenThereIsSRVLookup() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .serverSelectionTimeout(5, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2)).sleepTime(Duration.ofHours(1)));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        MongoTimeoutException exception = assertThrows(MongoTimeoutException.class, () -> cluster.selectServer(mock(ServerSelector.class)));
        assertEquals("Timed out after 5 ms while waiting to resolve SRV records for foo.bar.com.", exception.getMessage());
    }

    @Test
    public void shouldTimeoutSelectServerWhenThereIsSRVLookupException() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .serverSelectionTimeout(10, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2))
                        .sleepTime(Duration.ofMillis(1))
                        .exception(new MongoConfigurationException("Unable to resolve SRV record")));
        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        MongoTimeoutException exception = assertThrows(MongoTimeoutException.class, () -> cluster.selectServer(mock(ServerSelector.class)));
        assertEquals("Timed out after 10 ms while waiting to resolve SRV records for foo.bar.com. "
                        + "Resolution exception was 'com.mongodb.MongoConfigurationException: Unable to resolve SRV record'",
                exception.getMessage());
    }

    @Test
    public void shouldTimeoutSelectServerAsynchronouslyWhenThereIsSRVLookup() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings
                .builder()
                .serverSelectionTimeout(5, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2)).sleepTime(Duration.ofHours(1)));

        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
        cluster.selectServerAsync(mock(ServerSelector.class), callback);

        MongoTimeoutException exception = assertThrows(MongoTimeoutException.class, callback::get);
        assertEquals("Timed out after 5 ms while waiting to resolve SRV records for foo.bar.com.", exception.getMessage());
    }

    @Test
    public void shouldTimeoutSelectServerAsynchronouslyWhenThereIsSRVLookupException() {
        // given
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .serverSelectionTimeout(10, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2))
                        .sleepTime(Duration.ofMillis(1))
                        .exception(new MongoConfigurationException("Unable to resolve SRV record")));
        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
        cluster.selectServerAsync(mock(ServerSelector.class), callback);

        MongoTimeoutException exception = assertThrows(MongoTimeoutException.class, callback::get);
        assertEquals("Timed out after 10 ms while waiting to resolve SRV records for foo.bar.com. "
                        + "Resolution exception was 'com.mongodb.MongoConfigurationException: Unable to resolve SRV record'",
                exception.getMessage());
    }

    @Test
    void shouldNotInitServerAfterClosing() {
        // prepare mocks
        ClusterSettings clusterSettings = ClusterSettings.builder().mode(ClusterConnectionMode.LOAD_BALANCED).srvHost("foo.bar.com").build();
        ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
        when(serverFactory.getSettings()).thenReturn(mock(ServerSettings.class));
        DnsSrvRecordMonitorFactory srvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(srvRecordMonitorFactory.create(any(), eq(clusterSettings.getSrvServiceName()), any(DnsSrvRecordInitializer.class))).thenReturn(mock(DnsSrvRecordMonitor.class));
        ArgumentCaptor<DnsSrvRecordInitializer> serverInitializerCaptor = ArgumentCaptor.forClass(DnsSrvRecordInitializer.class);
        // create `cluster` and capture its `DnsSrvRecordInitializer` (server initializer)
        LoadBalancedCluster cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, srvRecordMonitorFactory);
        verify(srvRecordMonitorFactory, times(1)).create(any(), eq(clusterSettings.getSrvServiceName()), serverInitializerCaptor.capture());
        // close `cluster`, call `DnsSrvRecordInitializer.initialize` and check that it does not result in creating a `ClusterableServer`
        cluster.close();
        serverInitializerCaptor.getValue().initialize(Collections.singleton(new ServerAddress()));
        verify(serverFactory, never()).create(any(), any(), any(), any());
    }

    @Test
    void shouldCloseServerWhenClosing() {
        // prepare mocks
        ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
        when(serverFactory.getSettings()).thenReturn(mock(ServerSettings.class));
        ClusterableServer server = mock(ClusterableServer.class);
        when(serverFactory.create(any(), any(), any(), any())).thenReturn(server);
        // create `cluster` and check that it creates a `ClusterableServer`
        LoadBalancedCluster cluster = new LoadBalancedCluster(new ClusterId(),
                ClusterSettings.builder().mode(ClusterConnectionMode.LOAD_BALANCED).build(), serverFactory,
                mock(DnsSrvRecordMonitorFactory.class));
        verify(serverFactory, times(1)).create(any(), any(), any(), any());
        // close `cluster` and check that it closes `server`
        cluster.close();
        verify(server, atLeastOnce()).close();
    }

    @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
    @Tag("Slow")
    public void synchronousConcurrentTest() throws InterruptedException, ExecutionException, TimeoutException {
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .serverSelectionTimeout(5, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        Duration srvResolutionTime = Duration.ofSeconds(5);
        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> new TestDnsSrvRecordMonitor(invocation.getArgument(2)).sleepTime(srvResolutionTime));
        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        int numThreads = 100;
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            futures.add(executorService.submit(() -> {
                boolean success = false;
                while (!success) {
                    try {
                        cluster.selectServer(mock(ServerSelector.class));
                        success = true;
                    } catch (MongoTimeoutException e) {
                        // this is expected
                    }
                }
                // Keep going for a little while
                for (int j = 0; j < 100; j++) {
                    cluster.selectServer(mock(ServerSelector.class));
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get(10, SECONDS);
        }

        executorService.shutdownNow();
    }

    @RepeatedTest(value = 10, name = RepeatedTest.LONG_DISPLAY_NAME)
    @Tag("Slow")
    public void asynchronousConcurrentTest() throws InterruptedException, ExecutionException, TimeoutException {
        String srvHostName = "foo.bar.com";
        ServerAddress resolvedServerAddress = new ServerAddress("host1");
        ClusterableServer expectedServer = mock(ClusterableServer.class);

        ClusterSettings clusterSettings = ClusterSettings.builder()
                .serverSelectionTimeout(5, MILLISECONDS)
                .mode(ClusterConnectionMode.LOAD_BALANCED)
                .srvHost(srvHostName)
                .build();

        ClusterableServerFactory serverFactory = mockServerFactory(resolvedServerAddress, expectedServer);

        Duration srvResolutionTime = Duration.ofSeconds(5);
        DnsSrvRecordMonitorFactory dnsSrvRecordMonitorFactory = mock(DnsSrvRecordMonitorFactory.class);
        AtomicReference<TestDnsSrvRecordMonitor> dnsSrvRecordMonitorReference = new AtomicReference<>();
        when(dnsSrvRecordMonitorFactory.create(eq(srvHostName), eq(clusterSettings.getSrvServiceName()), any())).thenAnswer(
                invocation -> {
                    TestDnsSrvRecordMonitor dnsSrvRecordMonitor = new TestDnsSrvRecordMonitor(invocation.getArgument(2))
                            .sleepTime(srvResolutionTime);
                    dnsSrvRecordMonitorReference.set(dnsSrvRecordMonitor);
                    return dnsSrvRecordMonitor;
                });
        cluster = new LoadBalancedCluster(new ClusterId(), clusterSettings, serverFactory, dnsSrvRecordMonitorFactory);

        int numThreads = 10;
        List<List<FutureResultCallback<ServerTuple>>> callbacksList = new ArrayList<>(numThreads);
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>(numThreads);
        for (int i = 0; i < numThreads; i++) {
            List<FutureResultCallback<ServerTuple>> callbacks = new ArrayList<>();
            callbacksList.add(callbacks);
            futures.add(executorService.submit(() -> {
                while (!dnsSrvRecordMonitorReference.get().isInitialized()) {
                    FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
                    callbacks.add(callback);
                    cluster.selectServerAsync(mock(ServerSelector.class), callback);
                }
                // Keep going for a little while
                for (int j = 0; j < 100; j++) {
                    FutureResultCallback<ServerTuple> callback = new FutureResultCallback<>();
                    callbacks.add(callback);
                    cluster.selectServerAsync(mock(ServerSelector.class), callback);
                }
            }));
        }

        for (Future<?> future : futures) {
            future.get(10, SECONDS);
        }

        executorService.shutdownNow();

        for (List<FutureResultCallback<ServerTuple>> callbacks : callbacksList) {
            boolean foundFirstNonExceptionResult = false;
            for (FutureResultCallback<ServerTuple> curCallback : callbacks) {
                assertFalse(curCallback.wasInvokedMultipleTimes());
                assertTrue(curCallback.isDone());
                if (!curCallback.isCompletedExceptionally()) {
                    foundFirstNonExceptionResult = true;
                }
                if (foundFirstNonExceptionResult) {
                    assertFalse(curCallback.isCompletedExceptionally());
                }
            }
        }
    }

    private void assertServerTupleExpectations(final ServerAddress serverAddress, final ClusterableServer expectedServer,
                                               final ServerTuple serverTuple) {
        assertEquals(expectedServer, serverTuple.getServer());
        // Can't just use assertEquals here because the equals method compares lastUpdateTimeNanos property, which won't ever be the same
        ServerDescription serverDescription = serverTuple.getServerDescription();
        assertTrue(serverDescription.isOk());
        assertEquals(ServerConnectionState.CONNECTED, serverDescription.getState());
        assertEquals(serverAddress, serverDescription.getAddress());
        assertEquals(ServerType.LOAD_BALANCER, serverDescription.getType());
    }

    @NotNull
    private ClusterableServerFactory mockServerFactory(final ServerAddress serverAddress, final ClusterableServer expectedServer) {
        ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
        when(serverFactory.getSettings()).thenReturn(ServerSettings.builder().build());
        when(serverFactory.create(eq(serverAddress), any(), any(), any())).thenReturn(expectedServer);
        return serverFactory;
    }

    @NotNull
    private ClusterableServerFactory mockServerFactory() {
        ClusterableServerFactory serverFactory = mock(ClusterableServerFactory.class);
        when(serverFactory.getSettings()).thenReturn(ServerSettings.builder().build());
        return serverFactory;
    }

    private static class TestDnsSrvRecordMonitor implements DnsSrvRecordMonitor {
        private final DnsSrvRecordInitializer initializer;
        private Duration sleepTime;
        private Thread thread;
        private Collection<ServerAddress> hosts;
        private MongoException exception;
        private volatile boolean initialized;

        TestDnsSrvRecordMonitor(final DnsSrvRecordInitializer initializer) {
            this.initializer = initializer;
            sleepTime = Duration.ofMillis(50);
            hosts = Collections.singletonList(new ServerAddress("host1"));
        }

        TestDnsSrvRecordMonitor sleepTime(final Duration sleepTime) {
            this.sleepTime = sleepTime;
            return this;
        }

        TestDnsSrvRecordMonitor hosts(final Collection<ServerAddress> hosts) {
            this.hosts = hosts;
            return this;
        }

        public TestDnsSrvRecordMonitor exception(final MongoException exception) {
            this.exception = exception;
            return this;
        }

        public boolean isInitialized() {
            return initialized;
        }

        @Override
        public void start() {
            thread = new Thread(() -> {
                try {
                    Thread.sleep(sleepTime.toMillis());
                    if (exception != null) {
                        initializer.initialize(exception);
                    } else {
                        initializer.initialize(hosts);
                    }
                    initialized = true;
                } catch (InterruptedException e) {
                    // ignore
                }
            });
            thread.start();
        }

        @Override
        public void close() {
            if (thread != null) {
                thread.interrupt();
            }
        }
    }
}
