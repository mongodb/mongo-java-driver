/**
 * 
 */
package com.google.code.morphia.converters;

import java.util.Date;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({"rawtypes"})
public class DateConverter extends TypeConverter implements SimpleValueConverter{
	
	public DateConverter() { this(Date.class); };
	protected DateConverter(Class clazz) { super(clazz); }
	@SuppressWarnings("deprecation")
	@Override
	public
	Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		if (val instanceof Date)
			return val;

		if (val instanceof Number)
			return new Date(((Number)val).longValue());
			
		return new Date(Date.parse(val.toString())); // good luck
	}
}
