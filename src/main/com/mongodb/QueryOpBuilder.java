/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb;

/**
 * Utility for constructing Query operation command with query, orderby, hint, explain, snapshot.
 */
class QueryOpBuilder {

    static final String READ_PREFERENCE_META_OPERATOR = "$readPreference";

    private DBObject query;
	private DBObject orderBy;
	private DBObject hintObj;
	private String hintStr;
	private boolean explain;
	private boolean snapshot;
    private long maxTimeMS;
	private ReadPreference readPref;

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
     * Adds a read preference to the query operation
     *
     * @param readPref the read preference
     * @return this
     */
    public QueryOpBuilder addReadPreference(ReadPreference readPref) {
        this.readPref = readPref;
        return this;
    }

    public QueryOpBuilder addMaxTimeMS(final long maxTimeMS) {
        this.maxTimeMS = maxTimeMS;
        return this;
    }

    /**
	 * Constructs the query operation DBObject
	 * @return DBObject representing the query command to be sent to server
	 */
        public DBObject get() {
            DBObject lclQuery = query;

            //must always have a query
            if (lclQuery == null) {
                lclQuery = new BasicDBObject();
            }

            if (hasSpecialQueryFields()) {
                DBObject queryop = (specialFields == null ? new BasicDBObject() : specialFields);

                addToQueryObject(queryop, "$query", lclQuery, true);
                addToQueryObject(queryop, "$orderby", orderBy, false);
                if (hintStr != null)
                    addToQueryObject(queryop, "$hint", hintStr);
                if (hintObj != null)
                    addToQueryObject(queryop, "$hint", hintObj);

                if (explain)
                    queryop.put("$explain", true);
                if (snapshot)
                    queryop.put("$snapshot", true);
                if (readPref != null && readPref != ReadPreference.primary())
                    queryop.put(READ_PREFERENCE_META_OPERATOR, readPref.toDBObject());
                if (maxTimeMS > 0) {
                    queryop.put("$maxTimeMS", maxTimeMS);
                }

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

        if (maxTimeMS > 0) {
            return true;
        }

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
