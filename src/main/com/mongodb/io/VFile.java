// VFile.java

package com.mongodb.io;

import java.io.*;

public abstract class VFile {

    public static VFile create( File f ){
        return new VLocalFile( f );
    }

    protected VFile( VFile parent , String name ){
        _parent = parent;
        _name = name;
    }

    //  -------

    protected abstract long realLastModified();
    protected abstract boolean realExists();
    
    public abstract InputStream openInputStream()
        throws IOException;

    public abstract boolean isDirectory();

    // -----
    
    public final long lastModified(){
        if ( alwaysCheck() || _lastModifiedCache == null )
            _lastModifiedCache = realLastModified();
        return _lastModifiedCache;
    }

    public final boolean exists(){
        if ( alwaysCheck() || _existsCache == null )
            _existsCache = realExists();
        return _existsCache;
    }

    // -----
 
    protected boolean alwaysCheck(){
        return getMode() == Mode.PRODUCTION;
    }

    protected Mode getMode(){

        if ( _mode != null )
            return _mode;

        if ( _parent != null )
            return _parent.getMode();
        
        return Mode.DEVEL;
    }

    protected final String _name;
    protected final VFile _parent;

    private Mode _mode = null;

    private Long _lastModifiedCache;
    private Boolean _existsCache;

    static enum Mode { DEVEL , PRODUCTION };
}
