package com.google.code.morphia.query;

import org.bson.types.CodeWScope;

import com.mongodb.ReadPreference;

/**
 * @author Scott Hernandez
 */
public interface Query<T> extends QueryResults<T>, Cloneable {
	/**
	 * <p>Create a filter based on the specified condition and value.
	 * </p><p>
	 * <b>Note</b>: Property is in the form of "name op" ("age >").
	 * </p><p>
	 * Valid operators are ["=", "==","!=", "<>", ">", "<", ">=", "<=", "in", "nin", "all", "size", "exists"]
	 * </p>
	 * <p>Examples:</p>
	 * 
	 * <ul>
	 * <li>{@code filter("yearsOfOperation >", 5)}</li>
	 * <li>{@code filter("rooms.maxBeds >=", 2)}</li>
	 * <li>{@code filter("rooms.bathrooms exists", 1)}</li>
	 * <li>{@code filter("stars in", new Long[]{3,4}) //3 and 4 stars (midrange?)}</li>
	 * <li>{@code filter("age >=", age)}</li>
	 * <li>{@code filter("age =", age)}</li>
	 * <li>{@code filter("age", age)} (if no operator, = is assumed)</li>
	 * <li>{@code filter("age !=", age)}</li>
	 * <li>{@code filter("age in", ageList)}</li>
	 * <li>{@code filter("customers.loyaltyYears in", yearsList)}</li>
	 * </ul>
	 * 
	 * <p>You can filter on id properties <strong>if</strong> this query is
	 * restricted to a Class<T>.
	 */
	Query<T> filter(String condition, Object value);
	
	/** Fluent query interface: {@code createQuery(Ent.class).field("count").greaterThan(7)...} */
	FieldEnd<? extends Query<T>> field(String field);

	/** Criteria builder interface */
	FieldEnd<? extends CriteriaContainerImpl> criteria(String field);

	CriteriaContainer and(Criteria... criteria);
	CriteriaContainer or(Criteria... criteria);

	/** Limit the query using this javascript block; only one per query*/
    Query<T> where(String js);

    /** Limit the query using this javascript block; only one per query*/
    Query<T> where(CodeWScope js);
	
	/**
	 * <p>Sorts based on a property (defines return order).  Examples:</p>
	 * 
	 * <ul>
	 * <li>{@code order("age")}</li>
	 * <li>{@code order("-age")} (descending order)</li>
	 * <li>{@code order("age,date")}</li>
	 * <li>{@code order("age,-date")} (age ascending, date descending)</li>
	 * </ul>
	 */
	Query<T> order(String condition);
	
	/**
	 * Limit the fetched result set to a certain number of values.
	 * 
	 * @param value must be >= 0.  A value of 0 indicates no limit.
	 */
	Query<T> limit(int value);

	/**
	 * Batch-size of the fetched result (cursor).
	 * 
	 * @param value must be >= 0.  A value of 0 indicates the server default.
	 */
	Query<T> batchSize(int value);
	
	/**
	 * Starts the query results at a particular zero-based offset.
	 * 
	 * @param value must be >= 0
	 */
	Query<T> offset(int value);
	@Deprecated
	Query<T> skip(int value);
	
	/** Turns on validation (for all calls made after); by default validation is on*/
	Query<T> enableValidation();
	/** Turns off validation (for all calls made after)*/
	Query<T> disableValidation();
	
	/** Hints as to which index should be used.*/
	Query<T> hintIndex(String idxName);
	
	/** Limits the fields retrieved */
	Query<T> retrievedFields(boolean include, String...fields);

	/** Limits the fields retrieved to those of the query type -- dangerous with interfaces and abstract classes*/
	Query<T> retrieveKnownFields();

	/** Enabled snapshotted mode where duplicate results 
	 * (which may be updated during the lifetime of the cursor) 
	 *  will not be returned. Not compatible with order/sort and hint. **/
	Query<T> enableSnapshotMode();
	
	/** Disable snapshotted mode (default mode). This will be faster 
	 *  but changes made during the cursor may cause duplicates. **/
	Query<T> disableSnapshotMode();
	
	/** Route query to non-primary node  */
	Query<T> queryNonPrimary();

	/** Route query to primary node  */
	Query<T> queryPrimaryOnly();

	/** Route query ReadPreference */
	Query<T> useReadPreference(ReadPreference readPref);
	
	/** Disables cursor timeout on server. */
	Query<T> disableCursorTimeout();

	/** Enables cursor timeout on server. */
	Query<T> enableCursorTimeout();
	
	/**
	 * <p>Generates a string that consistently and uniquely specifies this query.  There
	 * is no way to convert this string back into a query and there is no guarantee that
	 * the string will be consistent across versions.</p>
	 * 
	 * <p>In particular, this value is useful as a key for a simple memcache query cache.</p>
	 */
	String toString();
	
	Class<T> getEntityClass();
	
	Query<T> clone();
}