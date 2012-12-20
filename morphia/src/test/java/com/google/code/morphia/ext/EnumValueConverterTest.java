package com.google.code.morphia.ext;

import junit.framework.Assert;

import org.bson.types.ObjectId;
import org.junit.Test;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Converters;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.converters.SimpleValueConverter;
import com.google.code.morphia.converters.TypeConverter;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.MappingException;
import com.mongodb.DBObject;

/**
 * Example converter which stores the enum value instead of string (name)
 * @author scotthernandez
 */
public class EnumValueConverterTest extends TestBase {

	@SuppressWarnings({"rawtypes", "unused"})
	static private class AEnumConverter extends TypeConverter implements SimpleValueConverter{
		
		public AEnumConverter() { super(AEnum.class); }
		
		@Override
		public
		Object decode(Class targetClass, Object fromDBObject, MappedField optionalExtraInfo) throws MappingException {
			if (fromDBObject == null) return null;
			return AEnum.values()[(Integer) fromDBObject];
		}
		
		@Override
		public
		Object encode(Object value, MappedField optionalExtraInfo) {
			if (value == null)
				return null;
			
			return ((Enum) value).ordinal();
		}
	}
	
	private static enum AEnum {
		One,
		Two
		
	}
	
	@SuppressWarnings("unused")
	@Converters(AEnumConverter.class)
	private static class EnumEntity {
		@Id ObjectId id = new ObjectId();
		AEnum val = AEnum.Two;
		
	}
	
	@Test
	public void testEnum() {
		EnumEntity ee = new EnumEntity();
		ds.save(ee);
		DBObject dbObj = ds.getCollection(EnumEntity.class).findOne();
		Assert.assertEquals(1, dbObj.get("val"));
	}
}
