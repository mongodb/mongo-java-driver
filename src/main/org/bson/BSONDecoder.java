// BSONDecoder.java

package org.bson;

import java.io.IOException;
import java.io.InputStream;

public interface BSONDecoder {
    
    public BSONObject readObject( byte[] b );
    
    public BSONObject readObject( InputStream in ) throws IOException;
    
    public int decode( byte[] b , BSONCallback callback );

    public int decode( InputStream in , BSONCallback callback ) throws IOException;

}
