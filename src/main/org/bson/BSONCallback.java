// BSONCallback.java

package org.bson;

import java.io.*;

public interface BSONCallback {
    
    void objectStart();
    void objectStart(String name);
    void objectDone();

}
