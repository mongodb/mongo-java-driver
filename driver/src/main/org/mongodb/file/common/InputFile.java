package org.mongodb.file.common;

public interface InputFile {

    /**
     * Associates a key with a value in the current map object.
     * 
     * @param key
     * @param value
     * 
     * @return the previous value for the key if any
     */
    Object put(final String key, final Object value);

    /**
     * Returns the value of a field on the object
     * 
     * @param string
     * @return the value object from the given key
     */
    Object get(final String string);

}