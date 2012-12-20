package com.google.code.morphia.query;

import com.mongodb.DBObject;

public interface Criteria {
	void addTo(DBObject obj);
	void attach(CriteriaContainerImpl container);
	String getFieldName();
}
