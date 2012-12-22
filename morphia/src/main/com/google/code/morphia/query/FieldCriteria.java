package com.google.code.morphia.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.utils.ReflectionUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class FieldCriteria extends AbstractCriteria implements Criteria {
	private static final Logr log = MorphiaLoggerFactory.get(FieldCriteria.class);

	protected final String field;
	protected final FilterOperator operator;
	protected final Object value;
	protected final boolean not;
	
	@SuppressWarnings("unchecked")
	protected FieldCriteria(QueryImpl<?> query, String field, FilterOperator op, Object value, boolean validateNames, boolean validateTypes) {
		this(query, field, op, value, validateNames, validateTypes, false);
	}
	protected FieldCriteria(QueryImpl<?> query, String field, FilterOperator op, Object value, boolean validateNames, boolean validateTypes, boolean not) {
		StringBuffer sb = new StringBuffer(field); //validate might modify prop string to translate java field name to db field name
		MappedField mf = Mapper.validate(query.getEntityClass(), query.getDatastore().getMapper(), sb, op, value, validateNames, validateTypes);
		field = sb.toString();

		Mapper mapr = query.getDatastore().getMapper();
		
		MappedClass mc = null;
		try {
			if (value != null && !ReflectionUtils.isPropertyType(value.getClass()) && !ReflectionUtils.implementsInterface(value.getClass(), Iterable.class))
				if (mf != null && !mf.isTypeMongoCompatible())
					mc = mapr.getMappedClass((mf.isSingleValue()) ? mf.getType() : mf.getSubClass());
				else
					mc = mapr.getMappedClass(value);
		} catch (Exception e) {
			//Ignore these. It is likely they related to mapping validation that is unimportant for queries (the query will fail/return-empty anyway)
			log.debug("Error during mapping of filter criteria: ", e);
		}
		
		Object mappedValue = mapr.toMongoObject(mf, mc, value);
		
		Class<?> type = (mappedValue == null) ?  null : mappedValue.getClass();
		
		//convert single values into lists for $in/$nin
		if (type != null && (op == FilterOperator.IN || op == FilterOperator.NOT_IN) && !type.isArray() && !Iterable.class.isAssignableFrom(type)) {
			mappedValue = Collections.singletonList(mappedValue);
		}
		
		//TODO: investigate and/or add option to control this.
		if (op == FilterOperator.ELEMENT_MATCH && mappedValue instanceof DBObject)
			((DBObject)mappedValue).removeField(Mapper.ID_KEY);
		
		this.field = field;
		this.operator = op;
		if (not)
			this.value = new BasicDBObject("$not", mappedValue);
		else
			this.value = mappedValue;
		this.not = not;
	}
	
	@SuppressWarnings("unchecked")
	public void addTo(DBObject obj) {
		if (FilterOperator.EQUAL.equals(operator)) {
			obj.put(this.field, value); // no operator, prop equals value
			
		} else {
			Object inner = obj.get(field); // operator within inner object

			if (!(inner instanceof Map)) {
				inner = new HashMap<String, Object>();
				obj.put(field, inner);
			}
			Object val = not ? new BasicDBObject("$not", value) : value;
			((Map<String, Object>)inner).put(operator.val(), val);
		}
	}
	
	public String getFieldName() {
		return field;
	}

	@Override
	public String toString() {
		return this.field + " " + this.operator.val() + " " + this.value;
	}
}
