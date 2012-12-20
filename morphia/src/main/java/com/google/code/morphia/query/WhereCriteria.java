package com.google.code.morphia.query;

import org.bson.types.CodeWScope;

import com.mongodb.DBObject;

public class WhereCriteria extends AbstractCriteria implements Criteria {

	private Object js;

	public WhereCriteria(String js) {
		this.js = js;
	}

	public WhereCriteria(CodeWScope js) {
		this.js = js;
	}

	public void addTo(DBObject obj) {
		obj.put(FilterOperator.WHERE.val(), this.js);
	}

	public String getFieldName() {
		return FilterOperator.WHERE.val();
	}

}
