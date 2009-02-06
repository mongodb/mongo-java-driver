package com.mongodb.util.gridfs;

import com.mongodb.Mongo;
import com.mongodb.BasicDBObject;
import com.mongodb.ObjectId;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBAddress;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileOutputStream;

/**
 *  Implementation of GridFS v1.0
 *
 *  <a href="http://www.mongodb.org/display/DOCS/GridFS+Specification">GridFS 1.0 spec</a>
 */
public class GridFS {

    public static final String DEFAULT_MIMETYPE = "application/octet-stream";
    public static final int DEFAULT_CHUNKSIZE = 256 * 1024;
    public static final String DEFAULT_BUCKET = "fs";

    protected final Mongo _mongo;
    protected final String _bucketName;
    protected final String _metadataCollectionName;
    protected final String _chunkCollectionName;

    /**
     * Creates a GridFS instance for the default bucket "fs"
     * in the given database.
     *
     * @param mongo database to work with
     */
    public GridFS(Mongo mongo) {
        this(mongo, DEFAULT_BUCKET);
    }

    /**
     * Creates a GridFS instance for the specified
     * in the given database.
     *
     * @param mongo database to work with
     * @param bucket bucket to use in the given database
     */
    public GridFS(Mongo mongo, String bucket) {
        _mongo = mongo;
        _bucketName = bucket;
        _metadataCollectionName = _bucketName + ".files";
        _chunkCollectionName = _bucketName + ".chunks";
    }

    /**
     *  Returns an object via a stream
     * @param obj object to Save
     * @throws IOException on error reading the stream
     */
    public void write(GridFSObject obj) throws IOException {

        _mongo.getCollection(_chunkCollectionName).ensureIndex(new BasicDBObject("files_id", 1).append("n", 1));
        _mongo.getCollection(_chunkCollectionName).ensureIndex(new BasicDBObject("n", 1));

        GridFSChunk chunk;
        long len = 0;

        while ((chunk = obj.getNextChunkFromStream()) != null) {

            len += chunk.getSize();
            saveChunk(chunk);
        }

        obj.setLength(len);
        saveMetadata(obj);
    }

    /**
     *   Gets the GridFSObject from the filestore based on name
     *
     * @param filename filename of the object to retrieve
     * @return objec
     */
    public GridFSObject read(String filename) {
        DBObject o = _mongo.getCollection(_metadataCollectionName).findOne(new BasicDBObject("filename", filename));

        if (o == null) {
            return null;
        }

        return read((ObjectId) o.get("_id"));
    }

    /**
     *   Gets the GridFSObject from the filestore based on id
     *
     * @param id id of the object to retrieve
     * @return objec
     */
    public GridFSObject read(ObjectId id) {

        DBObject o = _mongo.getCollection(_metadataCollectionName).findOne(new BasicDBObject("_id", id));

        if (o == null) {
            return null;
        }

        return new GridFSObject(this, o);
    }

    /**
     *   Returns a cursor for the chunks for a given file id
     *
     * @param id  id for the file
     * @return cursor for the chunks, in order
     */
    protected DBCursor getChunkCursorForFile(ObjectId id) {
        return _mongo.getCollection(_chunkCollectionName).find(new BasicDBObject("files_id", id)).sort(new BasicDBObject("n",1));
    }

    /**
     *  Saves the metadata for the FS object in the <i>bucketname</i>._files
     *  collection
     *
     * @param obj GridFSObject to save
     */
    protected void saveMetadata(GridFSObject obj) {
        _mongo.getCollection(_metadataCollectionName).insert(obj.getDBObject());
    }

    /**
     *  Saves a chunk for the FS object in the <i>bucketname</i>._chunks
     *  collection
     *
     * @param chunk chunk to save
     */
    protected void saveChunk(GridFSChunk chunk) {
        _mongo.getCollection(_chunkCollectionName).insert(chunk.getDBObject());
    }

    /**
     *   Returns a cursor for this filestore
     */
    public DBCursor getFileList() {
        return _mongo.getCollection(_metadataCollectionName).find().sort(new BasicDBObject("filename",1));
    }

    /**
     *  Dumps usage info to stdout
     */
    private static void printUsage() {
        System.out.println("Usage : [--bucket bucketname] action");
        System.out.println("  where  action is one of:");
        System.out.println("      list                      : lists all files in the store");
        System.out.println("      put filename              : puts the file filename into the store");
        System.out.println("      get filename1 filename2   : gets filename1 from store and sends to filename2");
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 1 || "--help".equals(args[0]) || "help".equals(args[0])) {
            printUsage();
            return;
        }

        for (int i=0; i < args.length; i++) {

            if (args[i].equals("list")) {

                GridFS fs = new GridFS(new Mongo(new DBAddress("127.0.0.1/org.mongo.gridfstest")));
                DBCursor cur = fs.getFileList();

                System.out.printf("%-60s %-10s\n", "Filename", "Length");
                
                while(cur.hasNext()) {
                    DBObject o = cur.next();
                    System.out.printf("%-60s %-10d\n", (String) o.get("filename"), ((Double) o.get("length")).longValue());
                }

            }

            if (args[i].equals("put")) {
                if (i == (args.length -1)) {
                    printUsage();
                }

                String filename = args[i+1];
                GridFS fs = new GridFS(new Mongo(new DBAddress("127.0.0.1/org.mongo.gridfstest")));
                GridFSObject fsobj = new GridFSObject(filename, new FileInputStream(new File(filename)));
                fs.write(fsobj);

                System.out.println("Saved : _id = " + fsobj.getID());
                return;
            }

            if (args[i].equals("get")) {
                if (i == (args.length -1)) {
                    printUsage();
                }

                String filename = args[i+1];
                String filename2 = args[i+2];

                GridFS fs = new GridFS(new Mongo(new DBAddress("127.0.0.1/org.mongo.gridfstest")));

                GridFSObject o = fs.read(filename);

                InputStream is = o.getInputStream();

                FileOutputStream fos = new FileOutputStream(new File(filename2));

                int c;
                byte[] buffer = new byte[DEFAULT_CHUNKSIZE];
                int count = 0;
                
                while ((c = is.read(buffer)) != -1) {
                   fos.write(buffer, 0, c);
                    count += c;
                }

                fos.close();

                System.err.println("Wrote " + count + " bytes to " + filename2);
                return;
            }
        }
    }
}
