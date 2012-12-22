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
public class LongConverter extends TypeConverter implements SimpleValueConverter {
	
	public LongConverter() { super(long.class, Long.class); }
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		if (val instanceof Number)
			return ((Number) val).longValue();
		else
			return Long.parseLong(val.toString());
	}

}
