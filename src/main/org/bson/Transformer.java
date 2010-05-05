// Transformer.java

package org.bson;

public interface Transformer {

    /**
     * @return the new object.  return passed in object if no change
     */
    public Object transform( Object o );
}
