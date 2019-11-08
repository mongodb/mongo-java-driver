package com.mongodb.reactivestreams.client.internal

import com.mongodb.Block
import com.mongodb.internal.async.SingleResultCallback
import com.mongodb.internal.async.client.gridfs.AsyncInputStream as WrappedAsyncInputStream
import com.mongodb.internal.async.client.gridfs.AsyncOutputStream as WrappedAsyncOutputStream
import com.mongodb.reactivestreams.client.TestSubscriber
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream
import com.mongodb.reactivestreams.client.gridfs.helpers.AsyncStreamHelper
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.nio.ByteBuffer

import static com.mongodb.internal.async.client.Observables.observe

class GridFSAsyncStreamHelperSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(1) }
    }

    def 'should call the underlying AsyncInputStream methods'() {
        given:
        def wrapped = Mock(WrappedAsyncInputStream)
        def byteBuffer = ByteBuffer.allocate(10)
        def stream = GridFSAsyncStreamHelper.toAsyncInputStream(wrapped)

        when:
        stream.read(byteBuffer).subscribe(subscriber)

        then:
        1 * wrapped.read(byteBuffer, _)

        when:
        stream.close().subscribe(subscriber)

        then:
        1 * wrapped.close(_)
    }

    def 'should call the underlying AsyncOutputStream methods'() {
        given:
        def wrapped = Mock(WrappedAsyncOutputStream)
        def byteBuffer = ByteBuffer.allocate(10)
        def stream = GridFSAsyncStreamHelper.toAsyncOutputStream(wrapped)

        when:
        stream.write(byteBuffer).subscribe(subscriber)

        then:
        1 * wrapped.write(byteBuffer, _)

        when:
        stream.close().subscribe(subscriber)

        then:
        1 * wrapped.close(_)
    }

    def 'should call the underlying async library AsyncInputStream methods'() {
        given:
        def wrapped = Mock(AsyncInputStream)
        def callback = Stub(SingleResultCallback)
        def publisher = Mock(Publisher)
        def byteBuffer = ByteBuffer.allocate(10)
        def stream = GridFSAsyncStreamHelper.toCallbackAsyncInputStream(wrapped)

        when:
        stream.read(byteBuffer, callback)

        then:
        1 * wrapped.read(byteBuffer) >> publisher

        then:
        1 * publisher.subscribe(_)

        when:
        stream.close(callback)

        then:
        1 * wrapped.close() >> publisher

        then:
        1 * publisher.subscribe(_)
    }

    def 'should call the underlying async library AsyncOutputStream methods'() {
        given:
        def wrapped = Mock(AsyncOutputStream)
        def callback = Stub(SingleResultCallback)
        def publisher = Mock(Publisher)
        def byteBuffer = ByteBuffer.allocate(10)
        def stream = GridFSAsyncStreamHelper.toCallbackAsyncOutputStream(wrapped)

        when:
        stream.write(byteBuffer, callback)

        then:
        1 * wrapped.write(byteBuffer) >> publisher

        then:
        1 * publisher.subscribe(_)

        when:
        stream.close(callback)

        then:
        1 * wrapped.close() >> publisher

        then:
        1 * publisher.subscribe(_)
    }

    def 'should pass the underlying InputStream values back'() {
        given:
        def inputStream = Mock(InputStream)
        def asyncInputStream = AsyncStreamHelper.toAsyncInputStream(inputStream)
        def callbackBasedInputStream = GridFSAsyncStreamHelper.toCallbackAsyncInputStream(asyncInputStream)

        when:
        def subscriber = new TestSubscriber<Integer>()
        new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callbackBasedInputStream.read(ByteBuffer.allocate(1024), callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  inputStream.read(_) >> { 42 }

        then:
        subscriber.assertReceivedOnNext([42])
        subscriber.assertTerminalEvent()

        when:
        subscriber = new TestSubscriber<Void>()
        new ObservableToPublisher<Void>(observe(new Block<SingleResultCallback<Void>>() {
            @Override
            void apply(final SingleResultCallback<Void> callback) {
                callbackBasedInputStream.close(callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  inputStream.close()

        then:
        subscriber.assertReceivedOnNext([])
        subscriber.assertTerminalEvent()
    }

    def 'should pass the underlying OutputStream values back'() {
        given:
        def outputStream = Mock(OutputStream)
        def asyncOutputStream = AsyncStreamHelper.toAsyncOutputStream(outputStream)
        def callbackBasedOutputStream = GridFSAsyncStreamHelper.toCallbackAsyncOutputStream(asyncOutputStream)

        when:
        def subscriber = new TestSubscriber<Integer>()
        new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callbackBasedOutputStream.write(ByteBuffer.allocate(1024), callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  outputStream.write(_)

        then:
        subscriber.assertReceivedOnNext([1024])
        subscriber.assertTerminalEvent()

        when:
        subscriber = new TestSubscriber<Void>()
        new ObservableToPublisher<Void>(observe(new Block<SingleResultCallback<Void>>() {
            @Override
            void apply(final SingleResultCallback<Void> callback) {
                callbackBasedOutputStream.close(callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  outputStream.close()

        then:
        subscriber.assertReceivedOnNext([])
        subscriber.assertTerminalEvent()
    }

    def 'should handle underlying InputStream errors'() {
        given:
        def inputStream = Mock(InputStream)
        def asyncInputStream = AsyncStreamHelper.toAsyncInputStream(inputStream)
        def callbackBasedInputStream = GridFSAsyncStreamHelper.toCallbackAsyncInputStream(asyncInputStream)

        when:
        def subscriber = new TestSubscriber<Integer>()
        new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callbackBasedInputStream.read(ByteBuffer.allocate(1024), callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  inputStream.read(_) >> { throw new IOException('Read failed') }

        then:
        subscriber.assertErrored()

        when:
        subscriber = new TestSubscriber<Void>()
        new ObservableToPublisher<Void>(observe(new Block<SingleResultCallback<Void>>() {
            @Override
            void apply(final SingleResultCallback<Void> callback) {
                callbackBasedInputStream.close(callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  inputStream.close() >> { throw new IOException('Closed failed') }

        then:
        subscriber.assertErrored()
    }

    def 'should handle underlying OutputStream errors'() {
        given:
        def outputStream = Mock(OutputStream)
        def asyncOutputStream = AsyncStreamHelper.toAsyncOutputStream(outputStream)
        def callbackBasedOutputStream = GridFSAsyncStreamHelper.toCallbackAsyncOutputStream(asyncOutputStream)

        when:
        def subscriber = new TestSubscriber<Integer>()
        new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
            @Override
            void apply(final SingleResultCallback<Integer> callback) {
                callbackBasedOutputStream.write(ByteBuffer.allocate(1024), callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  outputStream.write(_) >> { throw new IOException('Read failed') }

        then:
        subscriber.assertErrored()

        when:
        subscriber = new TestSubscriber<Void>()
        new ObservableToPublisher<Void>(observe(new Block<SingleResultCallback<Void>>() {
            @Override
            void apply(final SingleResultCallback<Void> callback) {
                callbackBasedOutputStream.close(callback)
            }
        })).subscribe(subscriber)
        subscriber.requestMore(1)

        then:
        1 *  outputStream.close() >> { throw new IOException('Closed failed') }

        then:
        subscriber.assertErrored()
    }


}
