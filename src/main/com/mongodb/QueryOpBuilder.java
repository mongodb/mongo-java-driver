package com.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


/**
 * Utility for constructing Query operation command with query, orderby, hint, explain, snapshot.
 */
class QueryOpBuilder {
	
	private DBObject query;
	private DBObject orderBy;
	private DBObject hintObj;
	private String hintStr;
	private boolean explain;
	private boolean snapshot;
	private DBObject readPref;

	private DBObject specialFields;
	
	public QueryOpBuilder(){
	}


	/**
	 * Adds the query clause to the operation
	 * @param query
	 * @return
	 */
	public QueryOpBuilder addQuery(DBObject query){
		this.query = query;
		return this;
	}
	
	/**
	 * Adds the orderby clause to the operation
	 * @param orderBy
	 * @return
	 */
	public QueryOpBuilder addOrderBy(DBObject orderBy){
		this.orderBy = orderBy;
		return this;	
	}
	
	/**
	 * Adds the hint clause to the operation
	 * @param hint
	 * @return
	 */
	public QueryOpBuilder addHint(String hint){
		this.hintStr = hint;
		return this;
	}
	
	/**
	 * Adds hint clause to the operation
	 * @param hint
	 * @return
	 */
	public QueryOpBuilder addHint(DBObject hint){
		this.hintObj = hint;
		return this;
	}
	
	
	/**
	 * Adds special fields to the operation
	 * @param specialFields
	 * @return
	 */
	public QueryOpBuilder addSpecialFields(DBObject specialFields){
		this.specialFields = specialFields;
		return this;
	}
	
	/**
	 * Adds the explain flag to the operation
	 * @param explain
	 * @return
	 */
	public QueryOpBuilder addExplain(boolean explain){
		this.explain = explain;
		return this;
	}
	
	/**
	 * Adds the snapshot flag to the operation
	 * @param snapshot
	 * @return
	 */
	public QueryOpBuilder addSnapshot(boolean snapshot){
		this.snapshot = snapshot;
		return this;
	}

    /**
     * Adds ReadPreference to the operation
     * @param readPref
     * @return
     */
    public QueryOpBuilder addReadPreference(DBObject readPref){
        this.readPref = readPref;
        return this;
    }
	
	/**
	 * Constructs the query operation DBObject
	 * @return DBObject representing the query command to be sent to server
	 */
	public DBObject get(){
		DBObject lclQuery = query;
		
		//must always have a query
		if(lclQuery == null){
			lclQuery = new BasicDBObject();
		}
		
		if (hasSpecialQueryFields()) {
			DBObject queryop = (specialFields == null ? new BasicDBObject() : specialFields);

            addToQueryObject(queryop, "query", lclQuery, true);
            addToQueryObject(queryop, "orderby", orderBy, false);
            if (hintStr != null)
                addToQueryObject(queryop, "$hint", hintStr);
            if (hintObj != null)
                addToQueryObject(queryop, "$hint", hintObj);

            if (explain)
                queryop.put("$explain", true);
            if (snapshot)
                queryop.put("$snapshot", true);
            if (readPref != null)
                queryop.put("$readPreference", readPref);

            return queryop;
		}
		
		return lclQuery;
	}

    private boolean hasSpecialQueryFields(){
        
        if ( readPref != null )
            return true;
        
        if ( specialFields != null )
            return true;

        if ( orderBy != null && orderBy.keySet().size() > 0 )
            return true;
        
        if ( hintStr != null || hintObj != null || snapshot || explain)
            return true;

        return false;
    }
	
	/**
	 * Adds DBObject to the operation
	 * @param dbobj DBObject to add field to
	 * @param field name of the field
	 * @param obj object to add to the operation.  Ignore if <code>null</code>.
	 * @param sendEmpty if <code>true</code> adds obj even if it's empty.  Ignore if <code>false</code> and obj is empty.
	 * @return
	 */
	private void addToQueryObject(DBObject dbobj, String field, DBObject obj, boolean sendEmpty) {
		if (obj == null)
			return;

		if (!sendEmpty && obj.keySet().size() == 0)
			return;

		addToQueryObject(dbobj, field, obj);
	}

	/**
	 * Adds an Object to the operation
	 * @param dbobj DBObject to add field to
	 * @param field name of the field
	 * @param obj Object to be added.  Ignore if <code>null</code>
	 * @return
	 */
	private void addToQueryObject(DBObject dbobj, String field, Object obj) {

		if (obj == null)
			return;

		dbobj.put(field, obj);
	}
	
	

}
