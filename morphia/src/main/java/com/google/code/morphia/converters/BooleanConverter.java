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
public class BooleanConverter extends TypeConverter implements SimpleValueConverter {

	public BooleanConverter() { super(boolean.class, Boolean.class); }
	
	@Override
	public Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;
		
		if (val instanceof Boolean)
			return (Boolean) val;

		//handle the case for things like the ok field
		if (val instanceof Number)
			return ((Number)val).doubleValue()==1D;

		String sVal = val.toString();
		return Boolean.parseBoolean(sVal);
	}
}
