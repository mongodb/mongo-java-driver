package com.google.code.morphia.query;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import java.util.Map;

public class GeoFieldCriteria extends FieldCriteria {

	Map<String, Object> opts = null;
	protected GeoFieldCriteria(QueryImpl<?> query, String field, FilterOperator op, Object value, boolean validateNames, boolean validateTypes, Map<String, Object> opts) {
		super(query, field, op, value, validateNames, validateTypes);
		this.opts = opts;
	}

	@Override
	public void addTo(DBObject obj) {
		BasicDBObjectBuilder query = null;
		switch (operator) {
			case NEAR:
				query = BasicDBObjectBuilder.start(FilterOperator.NEAR.val(), value);
				break;
			case NEAR_SPHERE:
				query = BasicDBObjectBuilder.start(FilterOperator.NEAR_SPHERE.val(), value);
				break;
			case WITHIN_BOX:
				query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
				break;
			case WITHIN_CIRCLE:
				query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
				break;
			case WITHIN_CIRCLE_SPHERE:
				query = BasicDBObjectBuilder.start().push(FilterOperator.WITHIN.val()).add(operator.val(), value);
				break;
			default:
				throw new UnsupportedOperationException(operator + " not supported for geo-query");
		}
		
		//add options...
		if (opts!=null)
			for (Map.Entry<String, Object> e : opts.entrySet())
				query.append(e.getKey(), e.getValue());
		
		obj.put(field, query.get());
	}
}
