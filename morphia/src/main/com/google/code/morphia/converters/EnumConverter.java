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
public class EnumConverter extends TypeConverter implements SimpleValueConverter{
	
	@Override
	protected
	boolean isSupported(Class c, MappedField optionalExtraInfo) {
		return c.isEnum();
	}
	
	@Override
	public
	Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo) throws MappingException {
		if (fromDBObject == null) return null;
		return Enum.valueOf(targetClass, fromDBObject.toString());
	}
	
	@Override
	public
	Object encode(Object value, MappedField optionalExtraInfo) {
		if (value == null)
			return null;
		
		return getName((Enum) value);
	}
	
	private <T extends Enum> String getName(T value) {
		return value.name();
	}
}
