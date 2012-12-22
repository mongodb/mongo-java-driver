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
public class ByteConverter extends TypeConverter implements SimpleValueConverter{
	public ByteConverter() { super(Byte.class, byte.class); }
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		if (val instanceof Number)
			return ((Number) val).byteValue();
		
		String sVal = val.toString();
		return Byte.parseByte(sVal);
	}
}
