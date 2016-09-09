/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client.gridfs.helpers

import com.mongodb.async.client.FunctionalSpecification
import com.mongodb.async.client.MongoCollection
import com.mongodb.async.client.MongoDatabase
import com.mongodb.async.client.gridfs.GridFSBucket
import com.mongodb.async.client.gridfs.GridFSBucketImpl
import org.bson.Document
import spock.lang.Requires

import java.nio.ByteBuffer
import java.nio.channels.AsynchronousByteChannel
import java.nio.channels.AsynchronousFileChannel
import java.nio.channels.CompletionHandler
import java.nio.file.Files

// Wildcard import to work around Java 1.6
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.Future

import static com.mongodb.async.client.Fixture.getDefaultDatabaseName
import static com.mongodb.async.client.Fixture.getMongoClient
import static com.mongodb.async.client.TestHelper.run
import static com.mongodb.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToInputStream

// Wildcard import to work around Java 1.6
import static com.mongodb.async.client.gridfs.helpers.AsynchronousChannelHelper.channelToOutputStream

@Requires({ javaVersion >= 1.7 })
class AsynchronousChannelHelperSmokeTestSpecification extends FunctionalSpecification {
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<Document> filesCollection;
    protected MongoCollection<Document> chunksCollection;
    protected GridFSBucket gridFSBucket;

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files')
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        run(filesCollection.&drop)
        run(chunksCollection.&drop)
        gridFSBucket = new GridFSBucketImpl(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            run(filesCollection.&drop)
            run(chunksCollection.&drop)
        }
    }

    @SuppressWarnings(['JavaIoPackageAccess', 'Println'])
    def 'should round trip a AsynchronousFileChannel'() {
        given:
        URI inputFileURI = getClass().getResource('/GridFSAsync/GridFSTestFile.txt').toURI()
        Path inputPath = Paths.get(inputFileURI)
        Path outputPath = Paths.get(inputPath.toString() + '.copy')

        when:
        def inputChannel = AsynchronousFileChannel.open(inputPath)
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', channelToInputStream(inputChannel))

        then:
        run(filesCollection.&count) == 1
        run(chunksCollection.&count) == 1

        when:
        def outputChannel = AsynchronousFileChannel.open(outputPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        run(gridFSBucket.&downloadToStream, objectId, channelToOutputStream(outputChannel))

        then:
        ByteBuffer.wrap(Files.readAllBytes(inputPath)) == ByteBuffer.wrap(Files.readAllBytes(outputPath))

        cleanup:
        if (outputPath) {
            try {
                Files.delete(outputPath)
            } catch (IOException e) {
                println(e.getMessage())
            }
        }
    }

    def 'should round trip a AsynchronousByteChannel'() {
        when:
        def content = ByteBuffer.wrap('Hello GridFS Byte Channels'.getBytes())
        def asyncByteChannel = new TestAsynchronousByteChannel(content)
        def objectId = run(gridFSBucket.&uploadFromStream, 'myFile', channelToInputStream(asyncByteChannel))

        then:
        run(filesCollection.&count) == 1
        run(chunksCollection.&count) == 1

        when:
        run(gridFSBucket.&downloadToStream, objectId, channelToOutputStream(asyncByteChannel))

        then:
        content == asyncByteChannel.getReadBuffer()
        content == asyncByteChannel.getWriteBuffer()
    }

    static class TestAsynchronousByteChannel implements AsynchronousByteChannel, Closeable {
        private boolean closed
        private final readBuffer
        private final writeBuffer = ByteBuffer.allocate(1024)

        TestAsynchronousByteChannel(final readBuffer) {
            this.readBuffer = readBuffer
        }

        ByteBuffer getReadBuffer() {
            readBuffer.flip()
        }

        ByteBuffer getWriteBuffer() {
            writeBuffer.flip()
        }

        @Override
        <A> void read(final ByteBuffer dst, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(dst.remaining(), readBuffer.remaining());
            if (transferAmount == 0) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = readBuffer.duplicate()
                temp.limit(temp.position() + transferAmount)
                dst.put(temp)
                readBuffer.position(readBuffer.position() + transferAmount)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> read(final ByteBuffer dst) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        <A> void write(final ByteBuffer src, final A attachment, final CompletionHandler<Integer, ? super A> handler) {
            int transferAmount = Math.min(src.remaining(), writeBuffer.remaining());
            if (transferAmount == 0) {
                transferAmount = -1
            }
            if (transferAmount > 0) {
                ByteBuffer temp = src.duplicate()
                temp.limit(temp.position() + transferAmount)
                writeBuffer.put(temp)
            }
            handler.completed(transferAmount, attachment)
        }

        @Override
        Future<Integer> write(final ByteBuffer src) {
            throw new UnsupportedOperationException('Not Supported')
        }

        @Override
        void close() throws IOException {
            closed = true
        }

        @Override
        boolean isOpen() {
            !closed;
        }
    }
}
