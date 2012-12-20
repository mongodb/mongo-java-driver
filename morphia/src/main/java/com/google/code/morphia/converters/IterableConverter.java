/**
 * 
 */
package com.google.code.morphia.converters;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.google.code.morphia.ObjectFactory;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.google.code.morphia.utils.ReflectionUtils;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */

@SuppressWarnings({"unchecked","rawtypes"})
public class IterableConverter extends TypeConverter {
	private final DefaultConverters chain;
	
	public IterableConverter(DefaultConverters chain) {
		this.chain = chain;
	}
	
	@Override
	protected
	boolean isSupported(Class c, MappedField mf) {
		if (mf != null)
			return mf.isMultipleValues() && !mf.isMap(); //&& !mf.isTypeMongoCompatible();
		else
			return c.isArray() || ReflectionUtils.implementsInterface(c, Iterable.class);
	}
	
	@Override
	public Object decode(Class targetClass, Object fromDBObject, MappedField mf) throws MappingException {
		if (mf == null || fromDBObject == null) return fromDBObject;
		
		Class subtypeDest = mf.getSubClass();
		Collection vals = createNewCollection(mf);
		
		if (fromDBObject.getClass().isArray()) {
			//This should never happen. The driver always returns list/arrays as a List
			for(Object o : (Object[])fromDBObject)
				vals.add(chain.decode( (subtypeDest != null) ? subtypeDest : o.getClass(), o));
		} else if (fromDBObject instanceof Iterable) {
			// map back to the java datatype
			// (List/Set/Array[])
			for (Object o : (Iterable) fromDBObject)
				vals.add(chain.decode((subtypeDest != null) ? subtypeDest : o.getClass(), o));
		} else {
			//Single value case.
			vals.add(chain.decode((subtypeDest != null) ? subtypeDest : fromDBObject.getClass(), fromDBObject));
		}

		//convert to and array if that is the destination type (not a list/set)
		if (mf.getType().isArray()) {
			return ReflectionUtils.convertToArray(subtypeDest, (ArrayList)vals);
		} else
			return vals;
	}

	private Collection<?> createNewCollection(final MappedField mf) {
		ObjectFactory of = mapr.getOptions().objectFactory;
		return mf.isSet() ? of.createSet(mf) : of.createList(mf);
	}
	
	@Override
	public
	Object encode(Object value, MappedField mf) {
		
		if (value == null)
			return null;
		
		Iterable<?> iterableValues = null;
		
		if (value.getClass().isArray()) {
			
			if (Array.getLength(value) == 0) {
				return value;
			}

			if (value.getClass().getComponentType().isPrimitive())
				return value;
			
			iterableValues = Arrays.asList((Object[]) value);
		} else {
			if (!(value instanceof Iterable))
				throw new ConverterException("Cannot cast " + value.getClass() + " to Iterable for MappedField: " + mf);
			
			// cast value to a common interface
			iterableValues = (Iterable<?>) value;
		}
		
		List values = new ArrayList();
		if (mf != null && mf.getSubClass() != null) {
			for (Object o : iterableValues) {
				values.add(chain.encode(mf.getSubClass(), o));
			}
		} else {
			for (Object o : iterableValues) {
				values.add(chain.encode(o));
			}
		}
		if (values.size() > 0 || mapr.getOptions().storeEmpties) {
			return values;
		} else
			return null;
	}
}
