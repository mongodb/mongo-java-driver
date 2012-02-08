// StreamUtil.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *  
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class StreamUtil {

    public static byte[] readBytesFully( InputStream is )
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        pipe( is , baos );
        return baos.toByteArray();
    }
    
    public static int pipe( InputStream is , OutputStream out )
        throws IOException {
        return pipe( is , out , -1 );
    }
    
    public static int pipe( InputStream is , OutputStream out , int maxSize )
        throws IOException {
        byte buf[] = new byte [4096];
        int len;
        int total = 0;
        while ((len = is.read(buf)) != -1){
            out.write(buf, 0, len); 
            total += len;
            if ( maxSize > 0 && total > maxSize )
                throw new IOException("too big");
        }
        return total;
    }

}
