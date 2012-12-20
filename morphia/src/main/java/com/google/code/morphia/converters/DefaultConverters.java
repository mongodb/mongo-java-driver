/**
 * 
 */
package com.google.code.morphia.converters;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.MapperOptions;
import com.google.code.morphia.mapping.MappingException;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Default encoders
 * 
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * @author scotthernandez
 */
@SuppressWarnings({"unchecked","rawtypes"})
public class DefaultConverters {
	private static final Logr log = MorphiaLoggerFactory.get(DefaultConverters.class);
	
	private List<TypeConverter> untypedTypeEncoders = new LinkedList<TypeConverter>();
	private Map<Class,List<TypeConverter>> tcMap = new ConcurrentHashMap<Class,List<TypeConverter>>();
	private List<Class<? extends TypeConverter>> registeredConverterClasses = new LinkedList<Class<? extends TypeConverter>>();
	
	private Mapper mapr;
	
	public DefaultConverters() {
		// some converters are commented out since the pass-through converter is enabled, at the end of the list.
		// Re-enable them if that changes.
//		addConverter(new PassthroughConverter(DBRef.class));

		//Pass-through DBObject or else the MapOfValuesConverter will process it.
		addConverter(new PassthroughConverter(DBObject.class, BasicDBObject.class));
		//Pass-through byte[] for the driver to handle
		addConverter(new PassthroughConverter(byte[].class));
		addConverter(new EnumSetConverter());
		addConverter(new EnumConverter());
		addConverter(new StringConverter());
		addConverter(new CharacterConverter());
		addConverter(new ByteConverter());
		addConverter(new BooleanConverter());
		addConverter(new DoubleConverter());
		addConverter(new FloatConverter());
		addConverter(new LongConverter());
		addConverter(new LocaleConverter());
		addConverter(new ShortConverter());
		addConverter(new IntegerConverter());
		addConverter(new SerializedObjectConverter());
		addConverter(new CharArrayConverter());
		addConverter(new DateConverter());
		addConverter(new URIConverter());
		addConverter(new KeyConverter());
		addConverter(new MapOfValuesConverter(this));
		addConverter(new IterableConverter(this));
		addConverter(new ClassConverter());
		addConverter(new ObjectIdConverter());
		addConverter(new TimestampConverter());

		//generic converter that will just pass things through.
		addConverter(new PassthroughConverter());
	}
	
	/**
	 * Add a type converter. If it is a duplicate for an existing type, it will override that type.
	 * @param tc
	 */
	public TypeConverter addConverter(TypeConverter tc) {
		if (tc.getSupportedTypes() != null)
			for(Class c : tc.getSupportedTypes())
				addTypedConverter(c, tc);
		else
			untypedTypeEncoders.add(tc);
		
		tc.setMapper(mapr);
		
		registeredConverterClasses.add(tc.getClass());
		return tc;
	}
	
	public TypeConverter addConverter(Class<? extends TypeConverter> clazz) {
		return addConverter((TypeConverter) this.mapr.getOptions().objectFactory.createInstance(clazz));
	}

	/**
	 * Removes the type converter.
	 * @param tc
	 */
	public void removeConverter(TypeConverter tc) {
		if (tc.getSupportedTypes() == null)
			untypedTypeEncoders.remove(tc);
		else
			for (List<TypeConverter> tcList : tcMap.values())
				if(tcList.contains(tc))
					tcList.remove(tc);
		
		registeredConverterClasses.remove(tc.getClass());		
	}

	public boolean isRegistered(Class<? extends TypeConverter> tcClass) {
		return registeredConverterClasses.contains(tcClass);
	}
	
	private void addTypedConverter(Class type, TypeConverter tc) {
		if (tcMap.containsKey(type)) {
			tcMap.get(type).add(0,tc);
			log.warning("Added duplicate converter for " + type + " ; " + tcMap.get(type));
		} else {
			ArrayList<TypeConverter> vals = new ArrayList<TypeConverter>();
			vals.add(tc);
			tcMap.put(type, vals);
		}
	}
	
	public void fromDBObject(final DBObject dbObj, final MappedField mf, final Object targetEntity) {
		Object object = mf.getDbObjectValue(dbObj);
		if (object == null) {
			processMissingField(mf);
		} else {
			TypeConverter enc = getEncoder(mf);
			Object decodedValue = enc.decode(mf.getType(), object, mf);
			try {
				mf.setFieldValue(targetEntity, decodedValue);
			} catch (IllegalArgumentException e) {
				throw new MappingException("Error setting value from converter (" +
						enc.getClass().getSimpleName() + ") for " + mf.getFullName() + " to " + decodedValue);
			}
		}
	}
	
	protected void processMissingField(final MappedField mf) {
		//we ignore missing values.
	}
	
	private TypeConverter getEncoder(MappedField mf) {
		return getEncoder(null, mf);
	}
	
	private TypeConverter getEncoder(Object val, MappedField mf) {
		
		List<TypeConverter> tcs = null;
		
		if (val != null)
			tcs = tcMap.get(val.getClass());
		
		if (tcs == null || (tcs.size() > 0 && tcs.get(0) instanceof PassthroughConverter)) 
			tcs = tcMap.get(mf.getType());
		
		if(tcs != null) {
			if (tcs.size() > 1)
				log.warning("Duplicate converter for " + mf.getType() + ", returning first one from " + tcs);
			return tcs.get(0);
		}
		
		for (TypeConverter tc : untypedTypeEncoders)
			if(tc.canHandle(mf) || (val != null && tc.isSupported(val.getClass(), mf)))
				return tc;
		
		throw new ConverterNotFoundException("Cannot find encoder for " + mf.getType() + " as need for "
				+ mf.getFullName());
	}
	
	private TypeConverter getEncoder(final Class c) {
		List<TypeConverter> tcs = tcMap.get(c);
		if(tcs != null) {
			if (tcs.size() > 1)
				log.warning("Duplicate converter for " + c + ", returning first one from " + tcs);
			return tcs.get(0);
		}
		
		for (TypeConverter tc : untypedTypeEncoders)
			if(tc.canHandle(c))
				return tc;
		
		throw new ConverterNotFoundException("Cannot find encoder for " + c.getName());
	}
	
	public void toDBObject(final Object containingObject, final MappedField mf, final DBObject dbObj, MapperOptions opts) {
		Object fieldValue = mf.getFieldValue(containingObject);
		TypeConverter enc = getEncoder(fieldValue, mf);
		
		Object encoded = enc.encode(fieldValue, mf);
		if (encoded != null || opts.storeNulls) {
			dbObj.put(mf.getNameToStore(), encoded);
		}
	}
	
	public Object decode(Class c, Object fromDBObject, MappedField mf) {
		if (c == null)
			c = fromDBObject.getClass();
		return getEncoder(c).decode(c, fromDBObject, mf);
	}
	
	public Object decode(Class c, Object fromDBObject) {
		return decode(c, fromDBObject, null);
	}
	
	public Object encode(Object o) {
		if (o == null)
			return null;
		return encode(o.getClass(), o);
	}
	
	public Object encode(Class c, Object o) {
		return getEncoder(c).encode(o);
	}

	public void setMapper(Mapper mapr) {
		this.mapr = mapr;
		for(List<TypeConverter> tcs : tcMap.values())
			for(TypeConverter tc : tcs)
				tc.setMapper(mapr);
		for(TypeConverter tc : untypedTypeEncoders)
			tc.setMapper(mapr);
	}
	
	public boolean hasSimpleValueConverter(MappedField c) {
		TypeConverter conv = getEncoder(c);
		return (conv instanceof SimpleValueConverter);
	}

	public boolean hasSimpleValueConverter(Class c) {
		TypeConverter conv = getEncoder(c);
		return (conv instanceof SimpleValueConverter);
	}

	public boolean hasSimpleValueConverter(Object o) {
		if (o == null) return false;
		if (o instanceof Class)
			return hasSimpleValueConverter((Class)o);
		else if (o instanceof MappedField)
			return hasSimpleValueConverter((MappedField)o);
		else
			return hasSimpleValueConverter(o.getClass());
	}
	

	public boolean hasDbObjectConverter(MappedField c) {
		TypeConverter conv = getEncoder(c);
		return conv != null && !(conv instanceof PassthroughConverter) && !(conv instanceof SimpleValueConverter);
	}
	
	public boolean hasDbObjectConverter(Class c) {
		TypeConverter conv = getEncoder(c);
		return conv != null && !(conv instanceof PassthroughConverter) && !(conv instanceof SimpleValueConverter);
	}
}
