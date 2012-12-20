/**
 * 
 */
package com.google.code.morphia.converters;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class PassthroughConverter extends TypeConverter {
	
	public PassthroughConverter() {}

	public PassthroughConverter(Class...types) { super(types); }
	
	@Override
	protected boolean isSupported(Class c, MappedField optionalExtraInfo) {
		return true;
	}
	
	@Override
	public Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo) throws MappingException {
		return fromDBObject;
	}
}
