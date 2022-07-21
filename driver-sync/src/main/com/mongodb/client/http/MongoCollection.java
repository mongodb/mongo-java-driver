package com.mongodb.client.http;
import com.mongodb.client.FindIterable;
import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

public class MongoCollection<TDoc> {

    private String collectionName;
    private String dbname;
    private String hostURL;
    public MongoCollection(String collectionName, String dbname, String hostURL) {
        this.collectionName = collectionName;
        this.dbname = dbname;
        this.hostURL = hostURL;
    }


    public FindIterable<TDoc> find(@Nullable Bson filter ) {
        return new FindIterator<TDoc, TDoc>(collectionName, dbname, filter, hostURL);
    }

    public FindIterable<TDoc> find( ) {
        return new FindIterator<TDoc, TDoc>(collectionName, dbname, null, hostURL);
    }
}
