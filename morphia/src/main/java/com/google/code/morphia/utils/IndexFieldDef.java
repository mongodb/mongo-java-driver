package com.google.code.morphia.utils;
@Deprecated
public class IndexFieldDef {
	String field;
	IndexDirection dir;

	/** Creates an ascending index of the field. */
	public IndexFieldDef(String field) {
		this(field, IndexDirection.ASC);
	}

	public IndexFieldDef(String field, IndexDirection dir) {
		this.field = field; this.dir = dir;
	}
	
	public String getField() {return field;}
	public IndexDirection getDirection() {return dir;}

	@Override
	public String toString() {
		return field + ":" + dir;
	}
	
	
}
