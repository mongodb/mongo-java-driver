/**
 *
 */
package org.bson;

import org.bson.io.BSONByteBuffer;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * @author scotthernandez
 * @deprecated Please use {@link com.mongodb.LazyDBList} instead.
 */
@Deprecated
public class LazyDBList extends LazyBSONList implements DBObject {
    private static final long serialVersionUID = -4415279469780082174L;

	public LazyDBList(byte[] data, LazyBSONCallback callback) { super(data, callback); }
	public LazyDBList(byte[] data, int offset, LazyBSONCallback callback) { super(data, offset, callback); }
	public LazyDBList(BSONByteBuffer buffer, LazyBSONCallback callback) { super(buffer, callback); }
	public LazyDBList(BSONByteBuffer buffer, int offset, LazyBSONCallback callback) { super(buffer, offset, callback); }

    /**
     * Returns a JSON serialization of this object
     * @return JSON serialization
     */
    @Override
    public String toString(){
        return JSON.serialize( this );
    }

    public boolean isPartialObject(){
        return _isPartialObject;
    }

    public void markAsPartialObject(){
        _isPartialObject = true;
    }

    private boolean _isPartialObject;
}
