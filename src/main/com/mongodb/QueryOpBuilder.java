package com.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


/**
 * Utility for constructing Query operation command with query, orderby, hint, explain, snapshot
 */
public class QueryOpBuilder {
	private DBObject queryop;

	/**
	 * Creates a builder with an empty query op
	 */
	public QueryOpBuilder(){
		queryop = new BasicDBObject();
	}
	
	/**
	 * Creates a new builder
	 * @param queryop
	 */
	public QueryOpBuilder(DBObject queryop){
		this.queryop = queryop;
	}
	
	/**
	 * Adds the query clause to the operation
	 * @param query
	 * @return
	 */
	public QueryOpBuilder addQuery(DBObject query){
		return addToQueryObject("query", query, true);
	}
	
	/**
	 * Adds the groupby clause to the operation
	 * @param orderBy
	 * @return
	 */
	public QueryOpBuilder addOrderBy(DBObject orderBy){
		return addToQueryObject("orderby", orderBy, false);	
	}
	
	/**
	 * Adds the hint clause to the operation
	 * @param hint
	 * @return
	 */
	public QueryOpBuilder addHint(String hint){
		if(hint != null)
			 addToQueryObject("$hint", hint);
		
		return this;
	}
	
	public QueryOpBuilder addHint(DBObject hint){
		if(hint != null)
			addToQueryObject("$hint", hint, false);
		
		return this;
	}
	
	/**
	 * Adds the explain flag to the operation
	 * @return
	 */
	public QueryOpBuilder addExplain(){
		return addToQueryObject("$explain", true);
	}
	
	/**
	 * Adds the snapshot flag to the operation
	 * @return
	 */
	public QueryOpBuilder addSnapshot(){
		return addToQueryObject("$snapshot", true);
		
	}

	/**
	 * Adds DBObject to the operation
	 * @param field name of the field
	 * @param obj object to add to the operation.  Ignore if <code>null</code>.
	 * @param sendEmpty if <code>true</code> adds obj even if it's empty.  Ignore if <code>false</code> and obj is empty.
	 * @return
	 */
	public QueryOpBuilder addToQueryObject(String field, DBObject obj, boolean sendEmpty) {
		if (obj == null)
			return this;

		if (!sendEmpty && obj.keySet().size() == 0)
			return this;

		return addToQueryObject(field, obj);
	}

	/**
	 * Adds an Object to the operation
	 * @param field name of the field
	 * @param obj Object to be added.  Ignore if <code>null</code>
	 * @return
	 */
	public QueryOpBuilder addToQueryObject(String field, Object obj) {

		if (obj == null)
			return this;

		queryop.put(field, obj);

		return this;
	}
	
	/**
	 * gets the constructed query operation object
	 * @return
	 */
	public DBObject get(){
		return queryop;
	}

}
