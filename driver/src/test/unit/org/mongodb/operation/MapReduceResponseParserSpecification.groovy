package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.MongoNamespace
import spock.lang.Specification
import spock.lang.Subject

class MapReduceResponseParserSpecification extends Specification {
    @Subject
    private final MapReduceResponseParser parser = new MapReduceResponseParser()

    def 'should parse a document containing database and collection names into a MongoNamespace'() {
        given:
        Document responseDocument = ['result': ['collection': 'theCollectionName', 'db': 'theDatabaseName'] as Document]
        String defaultDatabaseName = 'defaultDbName'

        when:
        MongoNamespace namespace = parser.getResultsNamespaceFromResponse(responseDocument, defaultDatabaseName)

        then:
        namespace != null
        namespace.collectionName == 'theCollectionName'
        namespace.databaseName == 'theDatabaseName'
    }

    //can the database really return a response like this?
    def 'should parse a document containing collection name and no database by using the current database'() {
        given:
        Document responseDocument = ['result': ['collection': 'theCollectionName'] as Document]
        String defaultDatabaseName = 'defaultDbName'

        when:
        MongoNamespace namespace = parser.getResultsNamespaceFromResponse(responseDocument, defaultDatabaseName)

        then:
        namespace != null
        namespace.collectionName == 'theCollectionName'
        namespace.databaseName == 'defaultDbName'
    }

    def 'should parse a result containing only a String of the collection name into a MongoNamespace'() {
        given:
        Document responseDocument = ['result': 'theCollectionName']
        String defaultDatabaseName = 'defaultDbName'

        when:
        MongoNamespace namespace = parser.getResultsNamespaceFromResponse(responseDocument, defaultDatabaseName)

        then:
        namespace != null
        namespace.collectionName == 'theCollectionName'
        namespace.databaseName == 'defaultDbName'
    }

}
