package com.mongodb.operation

import category.Async
import com.mongodb.Block
import com.mongodb.MongoCursor
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.MongoAsyncCursor
import com.mongodb.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static ParallelScanOptions.builder
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertTrue

@IgnoreIf( { isSharded() || !serverVersionAtLeast(asList(2, 6, 0)) } )
class ParallelScanOperationSpecification extends OperationFunctionalSpecification {
    Set<Integer> ids = [] as Set

    def 'setup'() {
        (1..2000).each {
            ids.add(it)
            getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', it))
        }
    }

    def 'should visit all documents'() {
        when:
        List<MongoCursor<Document>> cursors = new ParallelScanOperation<Document>(getNamespace(),
                                                                                  builder().numCursors(3).batchSize(500).build(),
                                                                                  new DocumentCodec())
                .execute(getBinding())

        then:
        cursors.size() <= 3

        when:
        for (MongoCursor<Document> cursor : cursors) {
            while (cursor.hasNext()) {
                Integer id = (Integer) cursor.next().get('_id')
                assertTrue(ids.remove(id))
            }
        }

        then:
        ids.isEmpty()
    }

    @Category(Async)
    def 'should visit all documents asynchronously'() {
        when:
        List<MongoAsyncCursor<Document>> cursors = new ParallelScanOperation<Document>(getNamespace(),
                                                                                       builder().numCursors(3).batchSize(500).build(),
                                                                                       new DocumentCodec())
                .executeAsync(getAsyncBinding()).get()

        then:
        cursors.size() <= 3

        when:
        for (MongoAsyncCursor<Document> cursor : cursors) {
            cursor.forEach(new Block<Document>() {
                @Override
                void apply(final Document document) {
                    Integer id = (Integer) document.get('_id')
                    assertTrue(ids.remove(id))
                }
            }).get()
        }

        then:
        ids.isEmpty()
    }
}