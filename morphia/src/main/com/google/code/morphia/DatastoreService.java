package com.google.code.morphia;

/**
 * 
 * @author Scott Hernnadez
 *
 */
public class DatastoreService {
	private static Morphia mor;
	private static Datastore ds;
	
	static {
		mor = new Morphia();
		ds = mor.createDatastore("test");
	}
	/** Connects to "test" database on localhost by default */
	public static Datastore getDatastore() {
		return ds;
	}

	public static void setDatabase(String dbName) {
		if (!((DatastoreImpl)ds).getDB().getName().equals(dbName)) ds = mor.createDatastore(dbName);
	} 
	
	@SuppressWarnings("unchecked")
	public static void mapClass(Class c) {
		mor.map(c);
	}

	@SuppressWarnings("unchecked")
	public static void mapClasses(Class[] classes) {
		for (Class c: classes)
			mapClass(c);
	}

	public static void mapPackage(String pkg) {
		mor.mapPackage(pkg, true);
	}
}