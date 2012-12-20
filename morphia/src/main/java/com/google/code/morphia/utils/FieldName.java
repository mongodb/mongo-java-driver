/**
 * 
 */
package com.google.code.morphia.utils;

import java.lang.reflect.Field;

/**
 * Handy class to test if a certain Fieldname is available in a class. Usage: If
 * you add <code>
 * public static final String _foo = FieldName.of("foo");
 * </code>
 * 
 * you´ll see an Exception on loading the class. A nice side-effect: if you use
 * this in Queries like <code>
 * q.field(MyEntity._foo).equal("bar")
 * </code>
 * 
 * your IDE is able to track this usage. Using FieldName does not at all replace
 * query validation.
 * 
 * @author us@thomas-daily.de
 * 
 */
public class FieldName {
	public static String of(final String name) {
		return of(callingClass(), name);
	}
	
	public static String of(final Class<?> clazz, final String name) {
		Assert.parameterNotNull(clazz, "clazz");
		Assert.parameterNotNull(name, "name");
		if (hasField(clazz, name))
			return name;
		throw new FieldNameNotFoundException("Field called '" + name + "' on class '" + clazz
 + "' was not found.");
	}
	
	private static boolean hasField(Class<?> clazz, String name) {
		Field[] fa = ReflectionUtils.getDeclaredAndInheritedFields(clazz, true);
		for (Field field : fa) {
			if (name.equals(field.getName()))
				return true;
		}
		return false;
	}
	
	private static Class<?> callingClass() throws java.lang.IllegalStateException {
		return callingClass(FieldName.class);
	}
	
	private static Class<?> callingClass(final Class<?>... classesToExclude) {
		final StackTraceElement[] stackTrace = new Exception().getStackTrace();
		for (int i = 0; i < stackTrace.length; i++) {
			final StackTraceElement e = stackTrace[i];
			final String c = e.getClassName();
			
			boolean exclude = false;
			for (final Class<?> ec : classesToExclude) {
				exclude |= c.equals(ec.getName());
			}
			if (!exclude) {
				return forName(c);
			}
		}
		throw new java.lang.IllegalStateException();
		
	}
	
	private static Class<?> forName(String c) {
		try {
			return Class.forName(c);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Error when getting class for name '" + c + "'");
		}
	};
	
	public static class FieldNameNotFoundException extends RuntimeException {
		public FieldNameNotFoundException(String msg) {
			super(msg);
		}
	}
	
}
