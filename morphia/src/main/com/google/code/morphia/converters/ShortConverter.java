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
public class ShortConverter extends TypeConverter implements SimpleValueConverter{
	public ShortConverter() { super(short.class, Short.class); }
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;
			Object dbValue = val;
		
		if (dbValue instanceof Number)
			return ((Number) dbValue).shortValue();
		
		String sVal = val.toString();
		return Short.parseShort(sVal);
	}
}
