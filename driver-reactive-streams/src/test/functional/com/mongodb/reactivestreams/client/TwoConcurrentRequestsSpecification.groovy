package com.mongodb.reactivestreams.client


import com.mongodb.MongoClientSettings
import com.mongodb.connection.TransportSettings
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings
import io.micrometer.observation.Observation
import io.micrometer.observation.ObservationHandler
import io.micrometer.observation.ObservationRegistry
import org.bson.Document
import reactor.core.publisher.Flux
import spock.lang.IgnoreIf

import java.time.Duration
import java.time.LocalTime
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors

import static com.mongodb.ClusterFixture.getServerApi

@IgnoreIf({ getServerApi() != null })
class TwoConcurrentRequestsSpecification extends FunctionalSpecification {
    MongoClient client
    MongoDatabase database
    MongoCollection<Document> collection
    String someCollectionName = 'testCollection'

    def setup() {

        def observationRegistry = ObservationRegistry.create()

        observationRegistry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {

            @Override
            void onStart(Observation.Context context) {
                context.put("startNanos", System.nanoTime())
                logMessage("started: ${context.name}")
            }

            @Override
            void onStop(Observation.Context context) {
                def startNanos = context.get("startNanos") as Long
                def elapsedMillis = startNanos == null ? -1 : (System.nanoTime() - startNanos) / 1_000_000
                logMessage("stopped: ${context.name} in ${elapsedMillis} ms")
            }

            @Override
            boolean supportsContext(Observation.Context context) {
                return true
            }
        })
        def micrometerSettings = MicrometerObservabilitySettings.builder()
                .observationRegistry(observationRegistry)
                .build()

        def threadCounter = new AtomicInteger(0)
        def threadFactory = { Runnable r ->
            def number = threadCounter.incrementAndGet()
            new Thread(r, "mongo-thread-" + number)
        } as ThreadFactory


//        def transportSettings =
//                TransportSettings
//                        .nettyBuilder()
//                        .eventLoopGroup(new MultiThreadIoEventLoopGroup(1, threadFactory, NioIoHandler.newFactory()))
//                        .build()

        def transportSettings =
                TransportSettings
                        .asyncBuilder()
                        .executorService(
                                new MonitoredExecutorService(
                                        Executors.newFixedThreadPool(1, threadFactory))
                        )
                        .build()

        def settings =
                MongoClientSettings
                        .builder()
                        .transportSettings(transportSettings)
                        .observabilitySettings(micrometerSettings)
                        .build()

        client = MongoClients.create(settings)
        database = client.getDatabase(getDatabaseName())

        Flux.from(database.createCollection(someCollectionName)).blockLast()
        collection = database.getCollection(someCollectionName)

        def document = new Document('name', 'test').append('value', 42)
        Flux.from(collection.insertOne(document)).blockLast()
        println "#### setup done"
        println ""

    }

    def cleanup() {
        println ""
        println "#### cleanup started"
        client?.close()
    }

    def 'should execute two commands'() {
        given:
        def filter = Document.parse('{ $where: "sleep(5000) || true" }')

        def runnable = {
            Flux
                    .from(collection.find(filter))
                    .collectList()
                    .map { docs ->
                        simulateCpuCodeForOperationIndex(1)
                        docs
                    }
                    .block()
        }

        when:
        def threadUser1 = new Thread(runnable, "user-thread-1")
        threadUser1.start()

        def threadUser2 = new Thread(runnable, "user-thread-2")
        threadUser2.start()

        then:
        threadUser1.join()
        threadUser2.join()
        true
    }

    static void logMessage(String msg) {
        def now = LocalTime.now()
        def thread = Thread.currentThread().name
        println "[${thread}] | ${now.hour}:${now.minute}:${now.second} | ${msg}"
    }

    static long simulateCpuCodeForOperationIndex(int index) {
        long startTime = System.nanoTime()
        long iteration = 0L

        while (Duration.ofNanos(System.nanoTime() - startTime).getSeconds() < 5) {
            iteration++;
            Math.sqrt((double) iteration); // busy cpu wait
        }

        return iteration
    }

    class MonitoredExecutorService implements ExecutorService {

        private final ExecutorService delegate;

        MonitoredExecutorService(ExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        void shutdown() {
            delegate.shutdown();
        }

        @Override
        List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(new MonitoredCallable<>(task));
        }

        @Override
        <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(new MonitoredRunnable(task), result);
        }

        @Override
        Future<?> submit(Runnable task) {
            return delegate.submit(new MonitoredRunnable(task));
        }

        @Override
        <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return delegate.invokeAll(tasks.stream()
                    .map(MonitoredCallable::new)
                    .collect(Collectors.toList()));
        }

        @Override
        <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.invokeAll(tasks.stream()
                    .map(MonitoredCallable::new)
                    .collect(Collectors.toList()), timeout, unit);
        }

        @Override
        <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return delegate.invokeAny(tasks.stream()
                    .map(MonitoredCallable::new)
                    .collect(Collectors.toList()));
        }

        @Override
        <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(tasks.stream()
                    .map(MonitoredCallable::new)
                    .collect(Collectors.toList()), timeout, unit);
        }

        @Override
        void execute(Runnable command) {
            delegate.execute(new MonitoredRunnable(command));
        }
    }

    static class MonitoredRunnable implements Runnable {

        private final Runnable delegate;

        private final long createdAtNanos = System.nanoTime();

        MonitoredRunnable(Runnable delegate) {
            this.delegate = delegate;
        }

        @Override
        public void run() {
            Duration waitTime = Duration.ofNanos(System.nanoTime() - createdAtNanos);
            logMessage("Runnable waited: " + waitTime.toMillis() + " ms");

            long startExecutionNanos = System.nanoTime();
            try {
                delegate.run();
            } finally {
                Duration executionTime = Duration.ofNanos(System.nanoTime() - startExecutionNanos);
                logMessage("Runnable executed in: " + executionTime.toMillis() + " ms");
            }
        }

    }

    class MonitoredCallable<V> implements Callable<V> {

        private final Callable<V> delegate;

        private final long createdAtNanos = System.nanoTime();

        MonitoredCallable(Callable<V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public V call() throws Exception {
            Duration waitTime = Duration.ofNanos(System.nanoTime() - createdAtNanos);
            logMessage("Callable waited: " + waitTime.toMillis() + " ms");

            long startExecutionNanos = System.nanoTime();
            try {
                return delegate.call();
            } finally {
                Duration executionTime = Duration.ofNanos(System.nanoTime() - startExecutionNanos);
                logMessage("Callable executed in: " + executionTime.toMillis() + " ms");
            }
        }
    }
}
