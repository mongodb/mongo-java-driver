package org.mongodb.file.util;

public enum ChunkSize {

    tiny_4K(4), // lots of small files only
    small_32K(32), // still small files mostly
    medium_256K(256), // good compromise, this is the default
    large_1M(1024), // for lots of larger files
    huge_4M(4096); // mega files only, no small files

    private int k;

    private ChunkSize(final int k) {

        this.k = k;
    }

    public int getChunkSize() {

        return (this.k * 1024) - BREATHING_ROOM;
    }

    // number of bytes to give as breathing room for other parts of the JSON
    // document in the chunks collection
    // and 1 k offset for power of 2 sizing
    private static final int BREATHING_ROOM = 1024 + 200;

}
