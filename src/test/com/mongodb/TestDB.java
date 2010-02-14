package com.mongodb;

/**
 * Simple DB setup for testing
 * @author Julson Lim
 *
 */
public class TestDB {
	
	private static final String HOST = "127.0.0.1";
	
	TestDB(String dbName) {
		try {
			_mongo = new Mongo(HOST);
			_db = _mongo.getDB(dbName);
			_db.dropDatabase();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public DBCollection getCollection(String collName) {
		DBCollection collection = _db.getCollection(collName);
		return collection;
	}
	
	public void cleanup() {
		_db.dropDatabase();
	}
	
	private final Mongo _mongo;
	private final DB _db;
}
