/**
 * 
 */
package com.google.code.morphia;

import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
public class AbstractEntityInterceptor implements EntityInterceptor {
	
	public void postLoad(Object ent, DBObject dbObj, Mapper mapr) {
	}
	
	public void postPersist(Object ent, DBObject dbObj, Mapper mapr) {
	}
	
	public void preLoad(Object ent, DBObject dbObj, Mapper mapr) {
	}
	
	public void prePersist(Object ent, DBObject dbObj, Mapper mapr) {
	}
	
	public void preSave(Object ent, DBObject dbObj, Mapper mapr) {
	}
}
