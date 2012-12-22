/**
 * 
 */
package com.google.code.morphia.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * 
 * @author Scott Hernandez
 */
public class UpdateOpsImpl<T> implements UpdateOperations<T> {
	Map<String, Map<String, Object>> ops = new HashMap<String, Map<String, Object>>();
	Mapper mapr;
	Class<T> clazz;
	boolean validateNames = true;
	boolean validateTypes = true;
	boolean isolated = false;
	
	public UpdateOpsImpl(Class<T> type, Mapper mapper) {
		this.mapr = mapper;
		this.clazz = type;
	}

	public UpdateOperations<T> enableValidation(){ validateNames = validateTypes = true; return this; }

	public UpdateOperations<T> disableValidation(){ validateNames = validateTypes = false; return this; }
	
	public UpdateOperations<T> isolated() { isolated = true; return this; }
	public boolean isIsolated() { return isolated; }
	
	@SuppressWarnings("unchecked")
	public void setOps(DBObject ops) {
		this.ops = (Map<String, Map<String, Object>>) ops;
	}
	public DBObject getOps() {
		return new BasicDBObject(ops);
	}

	public UpdateOperations<T> add(String fieldExpr, Object value) {
		return add(fieldExpr, value, false);
	}


	public UpdateOperations<T> add(String fieldExpr, Object value, boolean addDups) {
		if (value== null)
			throw new QueryException("Value cannot be null.");

//		Object dbObj = mapr.toMongoObject(value, true);
		add((addDups) ? UpdateOperator.PUSH : UpdateOperator.ADD_TO_SET, fieldExpr, value, true);
		return this;
	}

	public UpdateOperations<T> addAll(String fieldExpr, List<?> values, boolean addDups) {
		if (values == null || values.isEmpty())
			throw new QueryException("Values cannot be null or empty.");
		
//		List<?> convertedValues = (List<?>)mapr.toMongoObject(values, true);
		if(addDups)
			add(UpdateOperator.PUSH_ALL, fieldExpr, values, true);
		else
			add(UpdateOperator.ADD_TO_SET_EACH, fieldExpr, values, true);
		return this;
	}

	public UpdateOperations<T> dec(String fieldExpr) {
		return inc(fieldExpr, -1);
	}


	public UpdateOperations<T> inc(String fieldExpr) {
		return inc(fieldExpr, 1);
	}


	public UpdateOperations<T> inc(String fieldExpr, Number value) {
		if (value== null)
			throw new QueryException("Value cannot be null.");
		add(UpdateOperator.INC, fieldExpr, value, false);
		return this;
	}


	protected UpdateOperations<T> remove(String fieldExpr, boolean firstNotLast) {
		add(UpdateOperator.POP, fieldExpr, (firstNotLast) ? -1 : 1 , false);
		return this;
	}


	public UpdateOperations<T> removeAll(String fieldExpr, Object value) {
		if (value== null)
			throw new QueryException("Value cannot be null.");
//		Object dbObj = mapr.toMongoObject(value);
		add(UpdateOperator.PULL, fieldExpr, value, true);
		return this;
	}


	public UpdateOperations<T> removeAll(String fieldExpr, List<?> values) {
		if (values== null || values.isEmpty())
			throw new QueryException("Value cannot be null or empty.");
		
//		List<Object> vals = toDBObjList(values);
		add(UpdateOperator.PULL_ALL, fieldExpr, values, true);
		return this;
	}


	public UpdateOperations<T> removeFirst(String fieldExpr) {
		return remove(fieldExpr, true);
	}


	public UpdateOperations<T> removeLast(String fieldExpr) {
		return remove(fieldExpr, false);
	}

	public UpdateOperations<T> set(String fieldExpr, Object value) {
		if (value== null)
			throw new QueryException("Value cannot be null.");

//		Object dbObj = mapr.toMongoObject(value, true);
		add(UpdateOperator.SET, fieldExpr, value, true);
		return this;
	}

	public UpdateOperations<T> unset(String fieldExpr) {
		add(UpdateOperator.UNSET, fieldExpr, 1, false);
		return this;
	}
	
	protected List<Object> toDBObjList(MappedField mf, List<?> values){
		ArrayList<Object> vals = new ArrayList<Object>((int) (values.size()*1.3));
		for(Object obj : values)
			vals.add(mapr.toMongoObject(mf, null, obj));
		
		return vals;
	}
	
	//TODO Clean this up a little.
	protected void add(UpdateOperator op, String f, Object value, boolean convert) {
		if (value == null)
			throw new QueryException("Val cannot be null");

		Object val = null;
		MappedField mf = null;
		if (validateNames || validateTypes) {
			StringBuffer sb = new StringBuffer(f);
			mf = Mapper.validate(clazz, mapr, sb, FilterOperator.EQUAL, val, validateNames, validateTypes);
			f = sb.toString();
		}

		if (convert)
			if (UpdateOperator.PULL_ALL.equals(op) && value instanceof List)
				val = toDBObjList(mf, (List<?>) value);
			else
				val = mapr.toMongoObject(mf, null, value);

		
		if (UpdateOperator.ADD_TO_SET_EACH.equals(op))
			 val = new BasicDBObject(UpdateOperator.EACH.val(), val);
		
		if (val == null)
			val = value;
		
		String opString = op.val();

		if (!ops.containsKey(opString)) {
			ops.put(opString, new HashMap<String, Object>());
		}
		ops.get(opString).put(f,val);
	}
}
