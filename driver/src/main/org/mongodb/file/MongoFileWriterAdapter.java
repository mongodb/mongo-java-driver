package org.mongodb.file;

import org.mongodb.file.common.InputFile;
import org.mongodb.file.writing.ChunksStatisticsAdapter;

/**
 * Adapter to handle the custom parts of data collection from each chunk
 * 
 * @author David Buschman
 * 
 */
public class MongoFileWriterAdapter extends ChunksStatisticsAdapter {

    private MongoFile file;

    public MongoFileWriterAdapter(final MongoFile file) {

        super((InputFile) file);
        this.file = file;
    }

    @Override
    public void close() {

        super.close();
        file.save();
    }
}
