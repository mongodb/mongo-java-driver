package com.mongodb.gridfs;

import org.mongodb.file.writing.InputFile;

import com.mongodb.gridfs.io.ChunksStatisticsAdapter;

/**
 * Adapter to handle the custom pars of data collection from each chunk
 * 
 * @author David Buschman
 * 
 */
public class GridFSInputFileAdapter extends ChunksStatisticsAdapter {

    private GridFSInputFile file;

    public GridFSInputFileAdapter(final GridFSInputFile file) {

        super((InputFile) file);
        this.file = file;
    }

    @Override
    public void close() {

        super.close();
        file.superSave();
    }
}
