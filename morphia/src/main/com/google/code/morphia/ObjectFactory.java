package com.google.code.morphia;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

@SuppressWarnings("rawtypes")
public interface ObjectFactory {
	public Object createInstance(Class clazz);
	public Object createInstance(Class clazz, DBObject dbObj);
	public Object createInstance(Mapper mapr, MappedField mf, DBObject dbObj);
	public Map createMap(MappedField mf);
	public List createList(MappedField mf);
	public Set createSet(MappedField mf);
}
