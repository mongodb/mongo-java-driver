package com.google.code.morphia.query;

public interface FieldEnd<T> {
	FieldEnd<T> not();
	
	T exists();
	T doesNotExist();
	T greaterThan(Object val);	
	T greaterThanOrEq(Object val);	
	T lessThan(Object val);
	T lessThanOrEq(Object val);
	T equal(Object val);
	T notEqual(Object val);

	T startsWith(String prefix);
	T startsWithIgnoreCase(String prefix);
	T endsWith(String suffix);
	T endsWithIgnoreCase(String suffix);
	T contains(String string);
	T containsIgnoreCase(String suffix);
	
	T hasThisOne(Object val);
	T hasAllOf(Iterable<?> vals);
	T hasAnyOf(Iterable<?> vals);
	T hasNoneOf(Iterable<?> vals);

	T in(Iterable<?> vals);

	T notIn(Iterable<?> vals);

	T hasThisElement(Object val);
	T sizeEq(int val);
	
	T near(double x, double y);
	T near(double x, double y, boolean spherical);
	T near(double x, double y, double radius);
	T near(double x, double y, double radius, boolean spherical);
	T within(double x, double y, double radius);
	T within(double x, double y, double radius, boolean spherical);
	T within(double x1, double y1, double x2, double y2);
}
