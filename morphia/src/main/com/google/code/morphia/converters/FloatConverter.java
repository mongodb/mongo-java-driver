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
public class FloatConverter extends TypeConverter implements SimpleValueConverter{
	
	public FloatConverter() { super(Float.class, float.class); }
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		if (val instanceof Float) 
			return val;

		if (val instanceof Number) {
			return ((Number) val).floatValue();
		}
		String sVal = val.toString();
		return Float.parseFloat(sVal);
	}
}
