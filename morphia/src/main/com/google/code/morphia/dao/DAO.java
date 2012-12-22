package com.google.code.morphia.dao;

import java.util.List;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.QueryResults;
import com.google.code.morphia.query.UpdateOperations;
import com.google.code.morphia.query.UpdateResults;
import com.mongodb.DBCollection;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public interface DAO<T, K> {
	/** Starts a query for this DAO entities type*/
	public Query<T> createQuery();
	
	/** Starts a update-operations def for this DAO entities type*/
	public UpdateOperations<T> createUpdateOperations();
	
	/** The type of entities for this DAO*/
	public Class<T> getEntityClass();
	
	/** Saves the entity; either inserting or overriding the existing document */
	public Key<T> save(T entity);
	
	/** Saves the entity; either inserting or overriding the existing document */
	public Key<T> save(T entity, WriteConcern wc);
	
	/** Updates the first entity matched by the constraints with the modifiers supplied.*/
	public UpdateResults<T> updateFirst(Query<T> q, UpdateOperations<T> ops);
	
	/** Updates all entities matched by the constraints with the modifiers supplied.*/
	public UpdateResults<T> update(Query<T> q, UpdateOperations<T> ops);
	
	/** Deletes the entity */
	public WriteResult delete(T entity);
	
	/** Deletes the entity 
	 * @return */
	public WriteResult delete(T entity, WriteConcern wc);
	
	/** Delete the entity by id value */
	public WriteResult deleteById(K id);
	
	/** Saves the entities given the query*/
	public WriteResult deleteByQuery(Query<T> q);
	
	/** Loads the entity by id value*/
	public T get(K id);
	
	/** Finds the entities Key<T> by the criteria {key:value}*/
	public List<K> findIds(String key, Object value);
	
	/** Finds the entities Ts*/
	public List<K> findIds();
	
	/** Finds the entities Ts by the criteria {key:value}*/
	public List<K> findIds(Query<T> q);
	
	/** checks for entities which match criteria {key:value}*/
	public boolean exists(String key, Object value);
	
	/** checks for entities which match the criteria*/
	public boolean exists(Query<T> q);
	
	/** returns the total count*/
	public long count();
	
	/** returns the count which match criteria {key:value}*/
	public long count(String key, Object value);
	
	/** returns the count which match the criteria*/
	public long count(Query<T> q);
	
	/** returns the entity which match criteria {key:value}*/
	public T findOne(String key, Object value);
	
	/** returns the entity which match the criteria */
	public T findOne(Query<T> q);
	
	/** returns the entities */
	public QueryResults<T> find();
	
	/** returns the entities which match the criteria */
	public QueryResults<T> find(Query<T> q);

	/** ensures indexed for this DAO */
	public void ensureIndexes();

	/** gets the collection */
	public DBCollection getCollection();
	
	/** returns the underlying datastore */
	public Datastore getDatastore();
}