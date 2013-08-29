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

package org.mongodb.operation

import org.bson.BSONReader
import org.mongodb.Decoder
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.Index
import org.mongodb.MongoClientOptions
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

import static java.util.concurrent.Executors.newScheduledThreadPool
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSSLSettings
import static org.mongodb.OrderBy.ASC
import static org.mongodb.connection.ClusterConnectionMode.Single

class GetIndexesOperationSpecification extends FunctionalSpecification {
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

    def 'should return default index on Collection that exists'() {
        given:
        def operation = new GetIndexesOperation(bufferProvider, session, collection.getNamespace(), new DocumentCodec());
        collection.insert(new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<Document> indexes = operation.execute()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'
    }

    def 'should be able to use custom decoder for index information'() {
        given:
        def operation = new GetIndexesOperation(bufferProvider, session, collection.getNamespace(), new IndexDecoder());
        collection.insert(new Document('documentThat', 'forces creation of the Collection'))

        when:
        List<CustomIndex> indexes = operation.execute()

        then:
        indexes.size() == 1
        indexes[0].indexName == '_id_'
        indexes[0].indexField == '_id'
    }

    def 'should return created indexes on Collection'() {
        given:
        def operation = new GetIndexesOperation(bufferProvider, session, collection.getNamespace(), new DocumentCodec());
        collection.tools().ensureIndex(Index.builder().addKey('theField', ASC).build());

        when:
        List<Document> indexes = operation.execute()

        then:
        indexes.size() == 2
        indexes[0].name == '_id_'
        indexes[1].name == 'theField_1'
    }

    private final class CustomIndex {
        String indexName;
        String indexField;
    }

    private final class IndexDecoder implements Decoder<CustomIndex> {
        @Override
        CustomIndex decode(final BSONReader reader) {
            //index documents look something like:
/*          [v   : 1,
             key : [_id: 1],
             ns  : databaseName.collectionName,
             name: _id_
            ]
*/
            //read start of the index document
            reader.readStartDocument()
            //read and ignore field 'v'
            reader.readInt32('v')
            //read start of 'key' document
            reader.readStartDocument()
            //find the name of the field in the key document, this is the field name
            String indexField = reader.readName()
            //read and ignore value for key (this value is index type)
            reader.readInt32()
            //read end of key document
            reader.readEndDocument()
            //read and ignore the namespace field
            reader.readString('ns')
            //read the name of the index
            String indexName = reader.readString('name')

            CustomIndex index = new CustomIndex()
            index.indexName = indexName;
            index.indexField = indexField;
            index
        }
    }
}
