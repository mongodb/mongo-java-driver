// VLocalFile.java

package com.mongodb.io;

import java.io.*;

public class VLocalFile extends VFile {

    public VLocalFile( String f ){
        this( new File( f ) );
    }
    
    public VLocalFile( File f ){
        super( null , f.toString() );
        _file = f;
    }

    protected long realLastModified(){
        return _file.lastModified();
    }

    protected boolean realExists(){
        return _file.exists();
    }
    
    public InputStream openInputStream()
        throws IOException {
        return new FileInputStream( _file );
    }

    public boolean isDirectory(){
        return _file.isDirectory();
    }

    final File _file;
}
