package com.mongodb.client.http;
import com.mongodb.client.FindIterable;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

public class MongoCollection<TDoc> {

    private String collectionName;
    private String dbname;
    private static final Logger LOGGER = Loggers.getLogger("MongoClient");

    public MongoCollection(String collectionName, String dbname) {
        LOGGER.info("http MongoCollection 17");
        this.collectionName = collectionName;
        this.dbname = dbname;
    }


    public FindIterable<TDoc> find(@Nullable Bson filter ) {
        LOGGER.info("http MongoCollection 22");
        return new FindIterator<TDoc, TDoc>(collectionName, dbname, filter);
    }

    public FindIterable<TDoc> find( ) {
        LOGGER.info("http MongoCollection 22");
        return new FindIterator<TDoc, TDoc>(collectionName, dbname, null);
    }
}
