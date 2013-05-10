package org.mongodb.codecs;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ClassModel<T> {
    private static final Pattern FIELD_NAME_REGEX_PATTERN = Pattern.compile("([a-zA-Z_][\\w$]*)");

    private final Class<T> theClass;

    //Two collections to track the fields for performance reasons
    private final List<Field> validatedFields = new ArrayList<Field>();
    private final Map<String, Field> validatedFieldsByName = new HashMap<String, Field>();

    public ClassModel(final Class<T> theClass) {
        this.theClass = theClass;

        for (final Field field : theClass.getDeclaredFields()) {
            final String fieldName = field.getName();
            if (isValidFieldName(fieldName)) {
                this.validatedFields.add(field);
                this.validatedFieldsByName.put(fieldName, field);
            }
        }
    }

    public T createInstanceOfClass() throws IllegalAccessException {
        try {
            return theClass.newInstance();
        } catch (InstantiationException e) {
            throw new DecodingException(String.format("Can't create an instance of %s", theClass), e);
        }
    }

    public Collection<Field> getFields() {
        //returning validatedFieldsByName.values is half as fast as simply returning this list
        return validatedFields;
    }

    public Field getDeclaredField(final String fieldName) throws NoSuchFieldException {
        final Field field = validatedFieldsByName.get(fieldName);
        if (field == null) {
            throw new NoSuchFieldException(String.format("Field %s not found on class %s", fieldName, theClass));
        }
        return field;
    }

    private boolean isValidFieldName(final String fieldName) {
        //We need to document that fields starting with a $ will be ignored
        //and we probably need to be able to either disable this validation or make it pluggable
        return FIELD_NAME_REGEX_PATTERN.matcher(fieldName).matches();
    }

    @Override
    public String toString() {
        return "ClassModel{"
               + "theClass=" + theClass
               + ", validatedFields=" + validatedFields
               + '}';
    }
}
