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
public class ClassConverter extends TypeConverter implements SimpleValueConverter {

	public ClassConverter() { super(Class.class); }

	@Override
	public Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo) throws MappingException {
		if (fromDBObject == null)
            return null;

		String l = fromDBObject.toString();
		try
        {
            return Class.forName(l);
        }
        catch (ClassNotFoundException e)
        {
            throw new MappingException("Cannot create class from Name '"+l+"'",e);
        }
	}

	@Override
	public Object encode(Object value, MappedField optionalExtraInfo) {
		if (value == null)
            return null;
		else 
			return ((Class)value).getName();
	}
}
