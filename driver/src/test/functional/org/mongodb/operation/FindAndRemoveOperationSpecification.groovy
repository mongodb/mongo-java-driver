package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoClientOptions
import org.mongodb.MongoCollection
import org.mongodb.codecs.DocumentCodec
import org.mongodb.connection.Cluster
import org.mongodb.connection.ClusterSettings
import org.mongodb.connection.ClusterableServerFactory
import org.mongodb.connection.ConnectionFactory
import org.mongodb.connection.ServerAddress
import org.mongodb.connection.impl.DefaultClusterFactory
import org.mongodb.connection.impl.DefaultClusterableServerFactory
import org.mongodb.connection.impl.DefaultConnectionFactory
import org.mongodb.connection.impl.DefaultConnectionProviderFactory
import org.mongodb.session.ClusterSession
import org.mongodb.session.Session
import org.mongodb.test.Worker
import org.mongodb.test.WorkerCodec

import static java.util.concurrent.Executors.newScheduledThreadPool
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSSLSettings
import static org.mongodb.connection.ClusterConnectionMode.Single

class FindAndRemoveOperationSpecification extends FunctionalSpecification {
    private final DocumentCodec documentDecoder = new DocumentCodec()

    private final MongoClientOptions options = MongoClientOptions.builder().build();
    private final ConnectionFactory connectionFactory = new DefaultConnectionFactory(options.connectionSettings,
                                                                                     getSSLSettings(), getBufferProvider(), [])

    private final ClusterableServerFactory clusterableServerFactory = new DefaultClusterableServerFactory(
            options.serverSettings, new DefaultConnectionProviderFactory(options.connectionProviderSettings, connectionFactory),
            null, connectionFactory, newScheduledThreadPool(3), getBufferProvider())

    private final ClusterSettings clusterSettings = ClusterSettings.builder()
                                                                   .mode(Single)
                                                                   .hosts([new ServerAddress()])
                                                                   .build()
    private final Cluster cluster = new DefaultClusterFactory().create(clusterSettings, clusterableServerFactory)
    private final Session session = new ClusterSession(cluster)

    def 'should remove single document'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        collection.insert(pete);
        collection.insert(sam);

        when:
        FindAndRemove findAndRemove = new FindAndRemove().where(new Document('name', 'Pete'));

        FindAndRemoveOperation<Document> operation = new FindAndRemoveOperation<Document>(collection.namespace, findAndRemove,
                                                                                          documentDecoder, getBufferProvider(), session,
                                                                                          false)
        Document returnedDocument = operation.execute()

        then:
        collection.find().count() == 1;
        collection.find().one == sam
        returnedDocument == pete
    }

    def 'should remove single document when using custom codecs'() {
        given:
        MongoCollection<Worker> workerCollection = database.getCollection(getCollectionName(), new WorkerCodec())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)

        workerCollection.insert(pete);
        workerCollection.insert(sam);

        when:
        FindAndRemove<Worker> findAndRemove = new FindAndRemove<Worker>().where(new Document('name', 'Pete'));

        FindAndRemoveOperation<Worker> operation = new FindAndRemoveOperation<Worker>(collection.namespace, findAndRemove,
                                                                                      new WorkerCodec(), getBufferProvider(), session,
                                                                                      false)
        Worker returnedDocument = operation.execute()

        then:
        workerCollection.find().count() == 1;
        workerCollection.find().one == sam
        returnedDocument == pete
    }
}
