package org.bson.codecs.pojo;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.reflect.Modifier.isPublic;

public class PropertyReflectionUtils {
    private PropertyReflectionUtils() {}

    private static final String IS_PREFIX = "is";
    private static final String GET_PREFIX = "get";
    private static final String SET_PREFIX = "set";

    static boolean isGetter(final Method method) {
        if (method.getParameterTypes().length > 0) {
            return false;
        } else if (method.getName().startsWith(GET_PREFIX) && method.getName().length() > GET_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(GET_PREFIX.length()));
        } else if (method.getName().startsWith(IS_PREFIX) && method.getName().length() > IS_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(IS_PREFIX.length()));
        }
        return false;
    }

    static boolean isSetter(final Method method) {
        if (method.getName().startsWith(SET_PREFIX) && method.getName().length() > SET_PREFIX.length()
                && method.getParameterTypes().length == 1) {
            return Character.isUpperCase(method.getName().charAt(SET_PREFIX.length()));
        }
        return false;
    }

    static boolean isPropertyMethod(final Method method) {
        return isGetter(method) || isSetter(method);
    }

    static String toPropertyName(final Method method) {
        String name = method.getName();
        String propertyName = name.substring(name.startsWith(IS_PREFIX) ? 2 : 3, name.length());
        char[] chars = propertyName.toCharArray();
        chars[0] = Character.toLowerCase(chars[0]);
        return new String(chars);
    }

    static PropertyMethods getPropertyMethods(Class<?> clazz) {
        List<Method> setters = new ArrayList<Method>();
        List<Method> getters = new ArrayList<Method>();
        for (Method method : clazz.getDeclaredMethods()) {
            // Note that if you override a getter to provide a more specific return type, getting the declared methods
            // on the subclass will return the overridden method as well as the method that was overridden from
            // the super class. This original method is copied over into the subclass as a bridge method, so we're
            // excluding them here to avoid multiple getters of the same property with different return types
            if (isPublic(method.getModifiers()) && !method.isBridge()) {
                if (isGetter(method)) {
                    getters.add(method);
                } else if (isSetter(method)) {
                    // Setters are a bit more tricky - don't do anything fancy here
                    setters.add(method);
                }
            }
        }

        return new PropertyMethods(getters, setters);
    }

    public static class PropertyMethods {
        private final Collection<Method> getterMethods;
        private final Collection<Method> setterMethods;

        public PropertyMethods(Collection<Method> getterMethods, Collection<Method> setterMethods) {
            this.getterMethods = getterMethods;
            this.setterMethods = setterMethods;
        }

        public Collection<Method> getGetterMethods() {
            return getterMethods;
        }

        public Collection<Method> getSetterMethods() {
            return setterMethods;
        }
    }
}
