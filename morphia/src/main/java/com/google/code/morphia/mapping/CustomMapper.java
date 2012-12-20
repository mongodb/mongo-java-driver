/**
 * 
 */
package com.google.code.morphia.mapping;

import java.util.Map;

import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.DBObject;

/**
 * A CustomMapper if one that implements the methods needed to map to/from POJO/DBObject
 * @author skot
 *
 */
public interface CustomMapper {
	void toDBObject(Object entity, MappedField mf, DBObject dbObject, Map<Object, DBObject> involvedObjects, Mapper mapr);
	void fromDBObject(DBObject dbObject, MappedField mf, Object entity, EntityCache cache, Mapper mapr);
}
