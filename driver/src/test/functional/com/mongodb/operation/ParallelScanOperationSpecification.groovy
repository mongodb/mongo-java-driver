package com.mongodb.operation

import com.mongodb.Block
import com.mongodb.async.MongoAsyncCursor
import com.mongodb.client.FunctionalSpecification
import com.mongodb.codecs.DocumentCodec
import org.mongodb.Document
import org.mongodb.MongoCursor

import static com.mongodb.client.Fixture.getAsyncBinding
import static com.mongodb.client.Fixture.getBinding
import static com.mongodb.client.Fixture.isSharded
import static com.mongodb.client.Fixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertTrue
import static org.junit.Assume.assumeFalse
import static org.junit.Assume.assumeTrue
import static org.mongodb.ParallelScanOptions.builder

class ParallelScanOperationSpecification extends FunctionalSpecification {
    Set<Integer> ids = [] as Set

    def 'setup'() {
        assumeTrue(serverVersionAtLeast(asList(2, 6, 0)))
        assumeFalse(isSharded())

        (1..2000).each {
            ids.add(it)
            getCollectionHelper().insertDocuments(new Document('_id', it))
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