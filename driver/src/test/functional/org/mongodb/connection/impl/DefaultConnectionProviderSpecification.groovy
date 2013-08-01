package org.mongodb.connection.impl

import org.bson.ByteBuf
import org.mongodb.connection.Connection
import org.mongodb.connection.ConnectionFactory
import org.mongodb.connection.MongoSocketWriteException
import org.mongodb.connection.MongoTimeoutException
import org.mongodb.connection.MongoWaitQueueFullException
import org.mongodb.connection.ServerAddress
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Subject

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.mongodb.Fixture.getPrimary

class DefaultConnectionProviderSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = getPrimary()
    private final TestConnectionFactory connectionFactory = Spy(TestConnectionFactory);

    @Subject
    private DefaultConnectionProvider pool;

    def cleanup() {
        pool.close();
    }

    def 'should get non null connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        expect:
        pool.get() != null
    }

    def 'should reuse released connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        when:
        pool.get().close();
        pool.get();

        then:
        1 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should release a connection back into the pool on close, not close the underlying connection'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .build());

        when:
        pool.get().close();

        then:
        !connectionFactory.getCreatedConnections().get(0).isClosed();
    }

    def 'should throw if pool is exhausted'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder().maxSize(1).maxWaitQueueSize(1).build());

        when:
        Connection first = pool.get();

        then:
        first != null;

        when:
        pool.get();

        then:
        thrown(MongoTimeoutException)
    }

    def 'should throw on timeout'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxWaitTime(50, MILLISECONDS)
                                                     .build());
        pool.get();

        when:
        TimeoutTrackingConnectionGetter connectionGetter = new TimeoutTrackingConnectionGetter(pool);
        new Thread(connectionGetter).start();

        connectionGetter.latch.await();

        then:
        connectionGetter.gotTimeout;
    }

    @Ignore
    def 'should throw on wait queue full'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxWaitTime(1000, MILLISECONDS)
                                                     .build());

        pool.get();

        TimeoutTrackingConnectionGetter timeoutTrackingGetter = new TimeoutTrackingConnectionGetter(pool);
        Thread.sleep(100);
        new Thread(timeoutTrackingGetter).start();

        when:
        pool.get();

        then:
        thrown(MongoWaitQueueFullException)
    }

    def 'should expire connection after max life time'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS, connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1).maxWaitQueueSize(1).maxConnectionLifeTime(20, MILLISECONDS).build());

        when:
        pool.get().close();
        Thread.sleep(50);
        pool.get();

        then:
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should expire connection after max life time on close'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxConnectionLifeTime(20, MILLISECONDS).build());

        when:
        Connection connection = pool.get();
        Thread.sleep(50);
        connection.close();

        then:
        connectionFactory.getCreatedConnections().get(0).isClosed();
    }

    def 'should expire connection after max idle time'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxConnectionIdleTime(5, MILLISECONDS).build());

        when:
        Connection connection = pool.get();
        connection.close();
        Thread.sleep(50);
        pool.get();

        then:
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should close connection after expiration'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxConnectionLifeTime(20, MILLISECONDS).build());

        when:
        pool.get().close();
        Thread.sleep(50);
        pool.get();

        then:
        connectionFactory.getCreatedConnections().get(0).isClosed();
    }

    def 'should create new connection after expiration'() throws InterruptedException {
        given:
        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             connectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(1)
                                                     .maxWaitQueueSize(1)
                                                     .maxConnectionLifeTime(5, MILLISECONDS).build());

        when:
        pool.get().close();
        Thread.sleep(50);
        Connection secondConnection = pool.get();

        then:
        secondConnection != null
        2 * connectionFactory.create(SERVER_ADDRESS)
    }

    def 'should expire all connections after exception'() throws InterruptedException {
        given:
        int numberOfConnectionsCreated = 0;

        ConnectionFactory mockConnectionFactory = Mock()
        mockConnectionFactory.create(_) >> {
            numberOfConnectionsCreated++
            Mock(Connection) {
                sendMessage(_) >> { throw new MongoSocketWriteException('', SERVER_ADDRESS, new IOException()) }
            }
        }

        pool = new DefaultConnectionProvider(SERVER_ADDRESS,
                                             mockConnectionFactory,
                                             DefaultConnectionProviderSettings.builder()
                                                     .maxSize(2)
                                                     .maxWaitQueueSize(1)
                                                     .build());
        when:
        Connection c1 = pool.get();
        Connection c2 = pool.get();

        and:
        c2.sendMessage(Collections.<ByteBuf> emptyList());

        then:
        thrown(MongoSocketWriteException)

        and:
        c1.close();
        c2.close();
        pool.get();

        then:
        numberOfConnectionsCreated == 3
    }

}
