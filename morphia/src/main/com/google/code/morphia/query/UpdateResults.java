package com.google.code.morphia.query;

import com.mongodb.WriteResult;

public class UpdateResults<T> {
	private WriteResult wr;
	
	public UpdateResults(WriteResult wr) {
		this.wr = wr;
	}
	
	public String getError() {
		return wr.getLastError().getErrorMessage();
	}
	
	public boolean getHadError() {
		String error = getError();
		return error != null && !error.isEmpty();
	}
	
	/** @return true if updated, false if inserted or none effected*/
	public boolean getUpdatedExisting() {
		return wr.getLastError().containsField("updatedExisting") ? (Boolean)wr.getLastError().get("updatedExisting") : false;
	}
	
	/** @return number updated */
	public int getUpdatedCount() {
		return getUpdatedExisting() ? getN() : 0;
	}
	
	/** @return number of affected documents */
	protected int getN() {
		return wr.getLastError().containsField("n") ? ((Number)wr.getLastError().get("n")).intValue() : 0;
	}
	
	/** @return number inserted; this should be either 0/1. */
	public int getInsertedCount() {
		return !getUpdatedExisting() ? getN() : 0;
	}
	
	/** @return the new _id field if an insert/upsert was performed */
	public Object getNewId() {
		return getInsertedCount() == 1 && wr.getLastError().containsField("upserted") ? wr.getLastError().get("upserted") : null ;
	}
	
	/** @return the underlying data */
	public WriteResult getWriteResult() {return wr;}
} 
