/**
 * 
 */
package com.google.code.morphia.converters;

import java.net.URI;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;

/**
 * @author scotthernandez
 */
@SuppressWarnings({"rawtypes"})
public class URIConverter extends TypeConverter implements SimpleValueConverter{
	
	public URIConverter() { this(URI.class); };
	protected URIConverter(Class clazz) { super(clazz); }
	@Override
	public String encode(Object uri, MappedField optionalExtraInfo) {
		if (uri == null) return null;
		
		return ((URI)uri).toString().replace(".", "%46");
	}
	
	@Override
	public
	Object decode(Class targetClass, Object val, MappedField optionalExtraInfo) throws MappingException {
		if (val == null) return null;

		return URI.create(val.toString().replace("%46", "."));
	}
}
