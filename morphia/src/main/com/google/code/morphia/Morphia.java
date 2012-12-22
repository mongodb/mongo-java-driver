/**
 * Copyright (C) 2010 Olafur Gauti Gudmundsson
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 *
 * @author Olafur Gauti Gudmundsson
 * @author Scott Hernandez
 **/
@SuppressWarnings({"unchecked", "rawtypes"})
public class Morphia {
	private final Mapper mapper;

	public Morphia() {
        this(Collections.EMPTY_SET);
    }

	public Morphia( Set<Class> classesToMap ) {
        this.mapper = new Mapper();
        for (Class c : classesToMap) {
            map(c);
        }
    }
	
    public synchronized Morphia map(Class... entityClasses) {
    	if ( entityClasses != null && entityClasses.length > 0)
    		for(Class entityClass : entityClasses) {
		        if ( !mapper.isMapped(entityClass) ) {
		            mapper.addMappedClass(entityClass);
		        }
        }
        return this;
    }

    public synchronized Morphia mapPackageFromClass(Class clazz) {
        return mapPackage(clazz.getPackage().getName(), false);
    }

    /**
     * Tries to map all classes in the package specified. Fails if one of the classes is not valid for mapping.
     *
     * @param packageName
     *            the name of the package to process
     * @return the Morphia instance
     */
    public synchronized Morphia mapPackage(String packageName) {
        return mapPackage(packageName, false);
    }

    /**
     * Tries to map all classes in the package specified.
     *
     * @param packageName
     *            the name of the package to process
     * @param ignoreInvalidClasses
     *            specifies whether to ignore classes in the package that cannot be mapped
     * @return the Morphia instance
     */
    public synchronized Morphia mapPackage(String packageName, boolean ignoreInvalidClasses) {
        try {
            for (Class c : ReflectionUtils.getClasses(packageName)) {
                try {
                    Embedded embeddedAnn = ReflectionUtils.getClassEmbeddedAnnotation(c);
                    Entity enityAnn = ReflectionUtils.getClassEntityAnnotation(c);
                    if ( enityAnn != null || embeddedAnn != null ) {
                        map(c);
                    }
                } catch (MappingException ex) {
                    if (!ignoreInvalidClasses) {
                        throw ex;
                    }
                }
            }
            return this;
        } catch (IOException ioex) {
            throw new MappingException("Could not get map classes from package " + packageName, ioex);
        } catch (ClassNotFoundException cnfex) {
            throw new MappingException("Could not get map classes from package " + packageName, cnfex);
        }
    }

    /**
     * Check whether a specific class is mapped by this instance.
     *
     * @param entityClass
     *            the class we want to check
     * @return true if the class is mapped, else false
     */
    public boolean isMapped(Class entityClass) {
        return mapper.isMapped(entityClass);
    }

	public <T> T fromDBObject(Class<T> entityClass, DBObject dbObject) {
		return fromDBObject(entityClass, dbObject, mapper.createEntityCache());
	}
	
	public <T> T fromDBObject(Class<T> entityClass, DBObject dbObject, EntityCache cache) {
        if ( !entityClass.isInterface() && !mapper.isMapped(entityClass)) {
            throw new MappingException("Trying to map to an unmapped class: " + entityClass.getName());
        }
        try {
			return (T) mapper.fromDBObject(entityClass, dbObject, cache);
        } catch ( Exception e ) {
            throw new MappingException("Could not map entity from DBObject", e); }
    }

    public DBObject toDBObject( Object entity ) {
        try {
            return mapper.toDBObject(entity);
        } catch ( Exception e ) {
            throw new MappingException("Could not map entity to DBObject", e); }
    }

    public Mapper getMapper() { return this.mapper; }

    /** This will create a new Mongo instance; it is best to use a Mongo singleton instance */
    @Deprecated public Datastore createDatastore(String dbName) { 
    	return createDatastore(dbName, null, null);
	}
    
    /** This will create a new Mongo instance; it is best to use a Mongo singleton instance*/
    @Deprecated public Datastore createDatastore(String dbName, String user, char[] pw) {
    	try {
			return createDatastore(new Mongo(), dbName, user, pw);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
    }

    /** It is best to use a Mongo singleton instance here**/
    public Datastore createDatastore(Mongo mon, String dbName, String user, char[] pw) {
    	return new DatastoreImpl(this, mon, dbName, user, pw);
    }

    /** It is best to use a Mongo singleton instance here**/
	public Datastore createDatastore(Mongo mongo, String dbName) {
		return createDatastore(mongo, dbName, null, null);
	}
	
}
