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
public class IntegerConverter extends TypeConverter implements SimpleValueConverter {
	public IntegerConverter() {
		super(int.class, Integer.class);
	}
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		if (val instanceof Integer) 
			return val;
		
		if (val instanceof Number)
			return ((Number) val).intValue();
		else 
			return Integer.parseInt(val.toString());
	}
}
