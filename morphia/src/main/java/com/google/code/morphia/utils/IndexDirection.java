package com.google.code.morphia.utils;

public enum IndexDirection {
	ASC, 
	DESC,
	GEO2D;

	public Object toIndexValue() {
		if(this.name().equals(ASC.name()))
			return 1;
		else if(this.name().equals(DESC.name()))
			return -1;
		else if(this.name().equals(GEO2D.name()))
			return "2d";
		else
			throw new RuntimeException("Invalid!");
	}
}
