package com.mongodb.filebox;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.mongodb.gridfs.GridFSDBFile;

public class OperationResult {
	public boolean Ok;
    public String filename;
    public Object _id;
    public Date UploadDate;
    public long FileSize;
    public String MD5;
    public boolean hasResult;
    public Map metaData;
    public String ErrorMessage;
    public boolean hasError;
    public GridFSDBFile GridfsFile;
    public List<GridFSDBFile> GridfsFileList;
    //public Set Tags;
    //public String Path;
    
    public OperationResult()
    {
        
    }
}
