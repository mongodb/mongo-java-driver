package com.mongodb.client.http;

public class MongoClients {

/*
    * This class is a wrapper around the MongoClient class. It is used to
    * provide a Java API for the MongoDB driver.
 */
    public static MongoClient create(String httpId) {
        return new MongoClient(httpId);
    }
}
