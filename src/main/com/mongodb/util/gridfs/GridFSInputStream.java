package com.mongodb.util.gridfs;

import java.io.InputStream;
import java.io.IOException;

class GridFSInputStream extends InputStream {

    protected final GridFSObject _obj;

    protected byte[] _currentChunk = null;
    protected int pos = 0;
    protected boolean endOfStream = false;

    GridFSInputStream(GridFSObject o) {
        _obj = o;
    }

    public int read(byte[] dest) throws IOException {

        int destPos = 0;

        if (_currentChunk == null || pos >= _currentChunk.length) {
            if (!getNextChunk()) {
                return -1;
            }
        }
        
        while(true) {
            int remaining = _currentChunk.length - pos;
            int spaceAvail = dest.length - destPos;
            int bytesToCopy = remaining < spaceAvail ? remaining : spaceAvail;

            System.arraycopy(_currentChunk, pos, dest, destPos, bytesToCopy);
            destPos += bytesToCopy;
            pos += bytesToCopy;

            // full?

            if (destPos == dest.length) {
                return destPos;
            }

            // more bytes left in current chunk?

            if (pos < _currentChunk.length) {
                return destPos;
            }

            // no more data ?

            if (!getNextChunk()) {
                return destPos;
            }

            // else keep going!
        }
    }
    
    public int read() throws IOException {

        if (endOfStream) {
            return -1;
        }

        if (_currentChunk == null || pos >= _currentChunk.length) {

            if (!getNextChunk()) {
                return -1;
            }
        }

        return _currentChunk[pos++] & 0xFF;
    }

    protected boolean getNextChunk() {
        GridFSChunk c = _obj.getNextChunkFromDB();

        if (c  == null) {
            _currentChunk = null;
            pos = 0;
            endOfStream = true;
            return false;
        }

        _currentChunk = c._data;
        pos = 0;

        return true;
    }
}
