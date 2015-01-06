package com.mongodb.async.client

import com.mongodb.MongoNamespace
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.SingleResultCallback
import com.mongodb.async.client.gridfs.GridFS
import com.mongodb.async.client.gridfs.GridFSFile
import com.mongodb.client.result.DeleteResult
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.ObjectId
import spock.lang.Specification

import java.text.DateFormat

import static java.util.concurrent.TimeUnit.SECONDS


class GridFSSpecification extends Specification {

    final static int COMPLETION_TIMEOUT = 10
    final static int DEFAULT_CHUNK_SIZE = 32
    final static String BUCKET_NAME = 'functional-test'

    final DateFormat dateFormat = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT)

    MongoDatabase database
    MongoCollection filesCollection
    MongoCollection chunksCollection

    GridFS gridFS

    def setup() {
        database = Fixture.getDefaultDatabase()
        filesCollection = Fixture.initializeCollection(new MongoNamespace(database.getName(), BUCKET_NAME + '.files'))
        chunksCollection = Fixture.initializeCollection(new MongoNamespace(database.getName(), BUCKET_NAME + '.chunks'))
        gridFS = database.gridFS(BUCKET_NAME, DEFAULT_CHUNK_SIZE)
    }

    def cleanup() {
        Fixture.dropDatabase(database.getName())
    }

    def 'test create indexes'() {

        when: 'create required index'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<Void>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'read back the created indexes'
        FutureResultCallback<List<Document>> getIndexCallback = new FutureResultCallback<List<Document>>()
        chunksCollection.getIndexes(getIndexCallback)
        List<Document> chunksIndexes = getIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: 'there should be an index on the _id field and one on the files_id and n field'
        chunksIndexes.size() == 2
        Document idIndexKeys = chunksIndexes.get(0).get('key', Document)
        idIndexKeys.keySet().size() == 1
        idIndexKeys.get('_id') == 1
        Document chunksIndexKeys = chunksIndexes.get(1).get('key', Document)
        chunksIndexKeys.keySet().size() == 2
        chunksIndexKeys.get('files_id') == 1
        chunksIndexKeys.get('n') == 1
    }

    def 'insert a new GridFSFile and find it using GridFS API'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)
        int chunkSize = 4

        and: 'instantiate a new GridFSFile where all fields are explicitly set'
        GridFSFile gridFSFile = gridFS.createFile()
        gridFSFile.setContentType('text/plain')
        gridFSFile.setFilename('test-file.txt')
        ObjectId objectId = new ObjectId()
        gridFSFile.setId(objectId)
        gridFSFile.setChunkSize(chunkSize)
        gridFSFile.setMetadata(['meta_string': 'test', 'meta_number': 1])
        gridFSFile.setAliases(['alias1', 'alias2'])
        Date date = dateFormat.parse('Nov 4, 2003 8:14 PM')
        gridFSFile.setUploadDate(date)

        and: 'add exactly 3 whole chunks of generated data'
        byte[] bytes = testData(chunkSize * 3)
        gridFSFile.append(bytes)

        when: 'persist the GridFSFile'
        def insertCallback = new FutureResultCallback<Void>()
        gridFSFile.insert(insertCallback)
        insertCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'find it using the GridFS API'
        def findCallback = new FutureResultCallback<GridFSFile>()
        gridFS.findOne(objectId, findCallback)
        GridFSFile dbFile = findCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'get all the bytes'
        FutureResultCallback<Void> endCallback = new FutureResultCallback<Void>()
        ByteCallback byteCallback = new ByteCallback(dbFile.getLength() as int)
        dbFile.getBytes(byteCallback, endCallback)
        endCallback.get(COMPLETION_TIMEOUT, SECONDS)
        byte[] dbBytes = byteCallback.bytes

        then:
        dbFile.getLength() == 3 * chunkSize
        Arrays.equals(bytes, dbBytes)
        dbFile.getContentType() == gridFSFile.getContentType()
        dbFile.getFilename() == gridFSFile.getFilename()
        dbFile.getId() == objectId
        dbFile.getChunkSize() == gridFSFile.chunkSize
        dbFile.getUploadDate() == date
        dbFile.getAliases() == gridFSFile.getAliases()
        dbFile.getMetadata() == gridFSFile.getMetadata()
    }

    def 'insert a new GridFSFile using default values and assert the plain documents'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)
        int fileLength = 1.5 * DEFAULT_CHUNK_SIZE

        when: 'instantiate a new GridFS file with 1.5 chunks of data'
        GridFSFile gridFSFile = gridFS.createFile()
        gridFSFile.setFilename('test-file');
        gridFSFile.setMetadata(['meta_string': 'test', 'meta_number': 1])
        gridFSFile.setAliases(['alias1', 'alias2'])
        byte[] bytes = testData(fileLength)

        and: 'which is appended in chunks of length (chunkSize - 2) to test writing to offsets'
        int bytesToWrite = fileLength
        while (bytesToWrite > 0) {
            int bytesWritten = Math.min(bytesToWrite, DEFAULT_CHUNK_SIZE - 2)
            int offset = fileLength - bytesToWrite
            gridFSFile.append(Arrays.copyOfRange(bytes, offset, offset + bytesWritten))
            bytesToWrite -= bytesWritten
        }

        and: 'persist the GridFSFile'
        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>()
        gridFSFile.insert(insertCallback)
        insertCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'fetch the inserted files document'
        FutureResultCallback<Document> fileCallback = new FutureResultCallback<Document>()
        filesCollection.find(new Document(['_id': gridFSFile.getId()])).first(fileCallback)
        Document fileDocument = fileCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'fetch the inserted chunks documents'
        FutureResultCallback<List<Document>> chunksCallback = new FutureResultCallback<List<Document>>()
        chunksCollection.find(new Document(['files_id': gridFSFile.getId()])).into([], chunksCallback)
        List<Document> chunkDocuments = chunksCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: 'retrieved documents should match the GridFS spec'
        gridFSFile.getId() != null
        fileDocument.get('_id', ObjectId) == gridFSFile.getId()
        fileDocument.getLong('length') == fileLength
        fileDocument.getInteger('chunkSize') == DEFAULT_CHUNK_SIZE
        // upload date must be less then 30 seconds before now
        fileDocument.get('uploadDate', Date).time - new Date().time < 30000
        fileDocument.get('filename') == gridFSFile.getFilename()
        fileDocument.get('contentType') == gridFSFile.getContentType()
        fileDocument.get('metadata') == new Document(['meta_string': 'test', 'meta_number': 1])
        fileDocument.get('aliases') == gridFSFile.getAliases()

        chunkDocuments.size() == 2
        Document chunk1 = chunkDocuments.get(0)
        chunk1.get('_id', ObjectId) != null
        chunk1.get('files_id', ObjectId) == gridFSFile.getId()
        chunk1.getInteger('n') == 0
        Arrays.equals(((Binary) chunk1.get('data')).getData(), Arrays.copyOfRange(bytes, 0, DEFAULT_CHUNK_SIZE))

        Document chunk2 = chunkDocuments.get(1)
        chunk2.get('_id', ObjectId) != null
        chunk2.get('files_id', ObjectId) == gridFSFile.getId()
        chunk2.getInteger('n') == 1
        Arrays.equals(((Binary) chunk2.get('data')).getData(),
                    Arrays.copyOfRange(bytes, DEFAULT_CHUNK_SIZE, DEFAULT_CHUNK_SIZE * 1.5 as int))
    }

    def 'insert a new file, but only read a certain byte range from it'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)
        int fileLength = 1.5 * DEFAULT_CHUNK_SIZE

        and: 'a file is in the database'
        GridFSFile gridFSFile = gridFS.createFile()
        byte[] bytes = testData(fileLength)
        gridFSFile.append(bytes)
        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>()
        gridFSFile.insert(insertCallback)
        insertCallback.get(COMPLETION_TIMEOUT, SECONDS)

        when: 'the file is retrieved and a range of bytes is read'
        FutureResultCallback<GridFSFile> findCallback = new FutureResultCallback<GridFSFile>()
        gridFS.findOne(gridFSFile.getId(), findCallback)
        GridFSFile dbFile = findCallback.get(COMPLETION_TIMEOUT, SECONDS)
        ByteCallback byteCallback = new ByteCallback(8 - 4)
        FutureResultCallback<Void> endCallback = new FutureResultCallback<Void>();
        dbFile.getBytes(4, 8, byteCallback, endCallback)
        endCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: 'the correct range of bytes should be returned'
        Arrays.equals(byteCallback.bytes, Arrays.copyOfRange(bytes, 4, 8))
    }

    def 'insert multiple GridFSFiles and list them'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        when: '10 documents are inserted'
        insertDocuments(10, DEFAULT_CHUNK_SIZE * 2)

        and: 'retrieved via find method'
        FutureResultCallback<List<GridFSFile>> futureResultCallback = new FutureResultCallback<List<GridFSFile>>()
        gridFS.find(new Document(), null, futureResultCallback)
        List<GridFSFile> files = futureResultCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: '10 documents should be returned'
        files.size() == 10
    }

    def 'insert multiple GridFSFiles and list them using a sort query'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        when: '10 documents are inserted'
        insertDocuments(10, DEFAULT_CHUNK_SIZE * 2)

        and: 'retrieved via find method'
        FutureResultCallback<List<GridFSFile>> futureResultCallback = new FutureResultCallback<List<GridFSFile>>()
        gridFS.find(new Document(), new Document(['filename': -1]), futureResultCallback)
        List<GridFSFile> files = futureResultCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: '10 documents should be returned'
        files.size() == 10
        for (int i = 0; i < files.size(); i++) {
            GridFSFile file = files.get(i)
            file.getFilename() == 'file' + (file.length - i)
        }
    }

    def 'insert multiple GridFSFiles and list them using a query'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        when: '10 documents are inserted'
        insertDocuments(10, DEFAULT_CHUNK_SIZE * 2)

        and: 'retrieved via find method'
        FutureResultCallback<List<GridFSFile>> futureResultCallback = new FutureResultCallback<List<GridFSFile>>()
        gridFS.find(new Document(['filename': 'file5']), null, futureResultCallback)
        List<GridFSFile> files = futureResultCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: '1 document should be returned'
        files.size() == 1
        files.get(0).getFilename() == 'file5'
    }

    def 'insert a file, delete it and try to find it'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'at least one file is in the database'
        GridFSFile gridFSFile = gridFS.createFile()
        ObjectId objectId = new ObjectId()
        gridFSFile.setId(objectId)
        gridFSFile.append(testData(DEFAULT_CHUNK_SIZE * 2))
        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>()
        gridFSFile.insert(insertCallback)
        insertCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'the file can be found'
        FutureResultCallback<GridFSFile> findCallback = new FutureResultCallback<GridFSFile>()
        gridFS.findOne(objectId, findCallback)
        assert findCallback.get(COMPLETION_TIMEOUT, SECONDS) != null

        when: 'the file is deleted'
        FutureResultCallback<DeleteResult> deleteCallback = new FutureResultCallback<DeleteResult>()
        gridFS.deleteOne(objectId, deleteCallback)
        DeleteResult deleteResult = deleteCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'and tried to be found again'
        findCallback = new FutureResultCallback<GridFSFile>()
        gridFS.findOne(objectId, findCallback)
        GridFSFile deletedFile = findCallback.get(COMPLETION_TIMEOUT, SECONDS)

        then: 'one file should have been deleted and the file should not be found'
        deleteResult.getDeletedCount() == 1
        deletedFile == null
    }

    def 'instantiate a GridFS file, write data and then try to change chunk size'() {

        given:
        GridFSFile gridFSFile = gridFS.createFile()
        gridFSFile.append(testData(4))

        when:
        gridFSFile.setChunkSize(256)

        then:
        def e = thrown(RuntimeException)
        e.getMessage() == 'chunkSize may not bo modified after data has been written'
    }

    def 'append data after GridFSFile has been inserted'() {

        given: 'the required indexes have been created'
        FutureResultCallback<Void> createIndexCallback = new FutureResultCallback<>()
        gridFS.createIndex(createIndexCallback)
        createIndexCallback.get(COMPLETION_TIMEOUT, SECONDS)

        and: 'at least one file is in the database'
        GridFSFile gridFSFile = gridFS.createFile()
        ObjectId objectId = new ObjectId()
        gridFSFile.setId(objectId)
        gridFSFile.append(testData(DEFAULT_CHUNK_SIZE * 2))
        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>()
        gridFSFile.insert(insertCallback)
        insertCallback.get(COMPLETION_TIMEOUT, SECONDS)

        when:
        gridFSFile.append(testData(4))

        then:
        def e = thrown(RuntimeException)
        e.getMessage() == 'Data can not be appended after file has been inserted'
    }

    def 'insert empty file'() {

        given:
        GridFSFile gridFSFile = gridFS.createFile()

        when:
        gridFSFile.insert(new FutureResultCallback<Void>())

        then:
        def e = thrown(RuntimeException)
        e.getMessage() == 'GridFSFile has no data!'
    }

    private void insertDocuments(final int numberOfDocuments, final int filelength) {
        for (int i = 0; i < numberOfDocuments; i++) {
            GridFSFile gridFSFile = gridFS.createFile()
            gridFSFile.setFilename('file' + i)
            gridFSFile.append(testData(filelength))

            FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();
            gridFSFile.insert(futureResultCallback)
            futureResultCallback.get(COMPLETION_TIMEOUT, SECONDS)
        }
    }

    /**
     * Create test data with a certain length, which consists of numbers 0-9 in ascending order.
     * @param length the length in bytes
     * @return byte array
     */
    private static byte[] testData(int length) {
        StringBuilder stringBuilder = new StringBuilder(length)
        while (length > stringBuilder.length()) {
            stringBuilder.append(stringBuilder.length() % 10)
        }
        stringBuilder.toString().getBytes('UTF-8')
    }

    /**
     * SingleResultCallback implementation that appends arrays of bytes.
     */
    private static class ByteCallback implements SingleResultCallback<byte[]> {

        byte[] bytes
        int length = 0

        ByteCallback(final int length) {
            bytes = new byte[length]
        }

        @Override
        void onResult(byte[] result, Throwable t) {
            if (t) {
                throw t
            } else {
                System.arraycopy(result, 0, bytes, length, result.length)
                length += result.length
            }
        }
    }

}
