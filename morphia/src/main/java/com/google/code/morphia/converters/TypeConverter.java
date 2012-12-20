/**
 * 
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
@SuppressWarnings("rawtypes")
public abstract class TypeConverter {
	protected Mapper mapr;
	protected Class[] supportTypes = null;

	protected TypeConverter() {}
	protected TypeConverter(Class...types) {
		supportTypes = types;
	}
	
	/** returns list of supported convertable types */
	final Class[] getSupportedTypes() {
		return supportTypes;
	}
	
	/** checks if the class is supported for this converter. */
	final boolean canHandle(Class c) {
		return isSupported(c, null);
	}
	
	/** checks if the class is supported for this converter. */
	protected boolean isSupported(Class<?> c, MappedField optionalExtraInfo) { return false; }

	/** checks if the MappedField is supported for this converter. */
	final boolean canHandle(MappedField mf) {
		return isSupported(mf.getType(), mf);
	}
	
	/** decode the {@link DBObject} and provide the corresponding java (type-safe) object<br><b>NOTE: optionalExtraInfo might be null</b>**/
	public abstract Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo)
			throws MappingException;
	
	/** decode the {@link DBObject} and provide the corresponding java (type-safe) object **/
	public final Object decode(Class targetClass, Object fromDBObject) throws MappingException {
		return decode(targetClass, fromDBObject, null);
	}
	
	/** encode the type safe java object into the corresponding {@link DBObject}<br><b>NOTE: optionalExtraInfo might be null</b>**/
	public final Object encode(Object value) throws MappingException {
		return encode(value, null);
	}
	
	/** checks if Class f is in classes **/
	protected boolean oneOf(Class f, Class... classes) {
		return oneOfClases(f, classes);
	}

	/** checks if Class f is in classes **/
	protected boolean oneOfClases(Class f, Class[] classes) {
		for (Class c : classes) {
			if (c.equals(f))
				return true;
		}
		return false;
	}
	
	/** encode the (type-safe) java object into the corresponding {@link DBObject}**/
	public Object encode(Object value, MappedField optionalExtraInfo) {
		return value; // as a default impl
	}
	
	public void setMapper(Mapper mapr) {
		this.mapr = mapr;
	}
}
