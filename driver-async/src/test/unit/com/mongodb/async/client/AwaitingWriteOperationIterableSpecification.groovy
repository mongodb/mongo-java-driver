package com.mongodb.async.client

import com.mongodb.Block
import com.mongodb.MongoException
import com.mongodb.async.AsyncBatchCursor
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.operation.AsyncOperationExecutor
import com.mongodb.operation.AsyncWriteOperation
import org.bson.Document
import spock.lang.Specification

class AwaitingWriteOperationIterableSpecification extends Specification {

    def 'first should get first result when awaited operation has completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')])
        def delegateIterable = Stub(MongoIterable) {
            first(_) >> {
                it[0].onResult(1, null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        awaitingIterable.first(callback)
        def first = callback.get()

        then:
        first == 1

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        awaitingIterable.first(callback)
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'first should get first result when awaited operation has not completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')], true) // queue execution
        def delegateIterable = Stub(MongoIterable) {
            first(_) >> {
                it[0].onResult(1, null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        awaitingIterable.first(callback)
        executor.proceedWithWrite()
        def first = callback.get()

        then:
        first == 1

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        awaitingIterable.first(callback)
        executor.proceedWithWrite()
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'into should get results when awaited operation has completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')])
        def delegateIterable = Stub(MongoIterable) {
            into(_, _) >> {
                it[1].onResult([1, 2], null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        awaitingIterable.into([], callback)
        def list = callback.get()

        then:
        list == [1, 2]

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        awaitingIterable.into([], callback)
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'into should get results when awaited operation has not completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')], true) // queue execution
        def delegateIterable = Stub(MongoIterable) {
            into(_, _) >> {
                it[1].onResult([1, 2], null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        awaitingIterable.into([], callback)
        executor.proceedWithWrite()
        def first = callback.get()

        then:
        first == [1, 2]

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        awaitingIterable.into([], callback)
        executor.proceedWithWrite()
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'forEach should get results when awaited operation has completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')])
        def delegateIterable = Stub(MongoIterable) {
            forEach(_, _) >> {
                it[0].apply(1)
                it[0].apply(2)
                it[1].onResult(null, null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        def list = []
        awaitingIterable.forEach({ it -> list.add(it) }, callback)
        callback.get()

        then:
        list == [1, 2]

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        list = []
        awaitingIterable.forEach({ it -> list.add(it) }, callback)
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'forEach should get results when awaited operation has not completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')], true) // queue execution
        def delegateIterable = Stub(MongoIterable) {
            forEach(_, _) >> {
                it[0].apply(1)
                it[0].apply(2)
                it[1].onResult(null, null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        def list = []
        awaitingIterable.forEach({ it -> list.add(it) }, callback)
        executor.proceedWithWrite()
        callback.get()

        then:
        list == [1, 2]

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        list = []
        awaitingIterable.forEach({ it -> list.add(it) }, callback)
        executor.proceedWithWrite()
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'should map'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')], true)  // queue execution
        def delegateIterable = Stub(MongoIterable) {
            forEach(_, _) >> {
                it[0].apply(1)
                it[0].apply(2)
                it[1].onResult(null, null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def mappingIterable = awaitingIterable.map { it -> it.toString() }
        def callback = new FutureResultCallback()
        mappingIterable.into([], callback)
        executor.proceedWithWrite()
        def list = callback.get()

        then:
        list == ['1', '2']

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        mappingIterable = awaitingIterable.map { it -> it.toString() }
        mappingIterable.into([], callback)
        executor.proceedWithWrite()
        callback.get()

        then:
        thrown(MongoException)
    }

    def 'batchCursor should get results when awaited operation has completed'() {
        given:
        def writeOp = Stub(AsyncWriteOperation)
        def executor = new TestOperationExecutor(['Response', new MongoException('Failed')], true)  // queue execution
        def cannedResults = [new Document('_id', 1), new Document('_id', 1), new Document('_id', 1)]
        def cursor = {
            Stub(AsyncBatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next(_) >> {
                    it[0].onResult(getResult(), null)
                }
                isClosed() >> { count >= 1 }
            }
        }
        def delegateIterable = Stub(MongoIterable) {
            batchCursor(_) >> {
                it[0].onResult(cursor(), null)
            }
        }

        when: 'write operation succeeded'
        def awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        def callback = new FutureResultCallback()
        awaitingIterable.batchCursor(callback)
        executor.proceedWithWrite()
        def batchCursor = callback.get()

        then:
        !batchCursor.isClosed()

        when:
        callback = new FutureResultCallback()
        batchCursor.next(callback)

        then:
        callback.get() == cannedResults
        batchCursor.isClosed()

        when: 'write operation failed'
        awaitingIterable = new AwaitingWriteOperationIterable(writeOp, executor, delegateIterable)
        callback = new FutureResultCallback()
        awaitingIterable.batchCursor(callback)
        executor.proceedWithWrite()
        batchCursor = callback.get()

        then:
        thrown(MongoException)
    }

    def 'should check variables using notNull'() {
        given:
        def mongoIterable = new AwaitingWriteOperationIterable(Stub(AsyncWriteOperation), Stub(AsyncOperationExecutor),
                Stub(MongoIterable))
        def callback = Stub(SingleResultCallback)
        def block = Stub(Block)
        def target = Stub(List)

        when:
        mongoIterable.first(null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.into(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.into(target, null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.forEach(null, callback)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.forEach(block, null)

        then:
        thrown(IllegalArgumentException)

        when:
        mongoIterable.map()

        then:
        thrown(IllegalArgumentException)
    }

}
