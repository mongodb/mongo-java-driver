package com.mongodb.filebox;

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

import java.io.InputStream;
import java.util.List;
import java.util.Map;


public class Filebox {
	private MongoClient moClient;
    private MongoClientURI moClientURI;
    private DB moDB;
    GridFS gridfs;
    private String URI;
    private boolean isConnOpen;
    public boolean autoConnectionOpen;
    public Object Source;
    
    public MongoClientOptions Options(){
        return moClient.getMongoClientOptions();
    }
    
    private void initProps(){                
        URI = "";
        isConnOpen = false;
        autoConnectionOpen = true;
    }
    
    public Filebox() {
        initProps();
    }
    
    public Filebox(String URI) throws Exception {
        initProps();
        this.setURI(URI);
    }
    
    private boolean setURI(String newURI) throws Exception {
        URI = newURI;
        return (URI == null ? newURI == null : URI.equals(newURI));
    }
    
    private String getURI() throws Exception {
        return URI;
    }
    
    public boolean isOpen() throws Exception {
        return isConnOpen;
    }
       
    public void Open() throws Exception{
        if (this.isOpen())
            throw new Exception("ConnectionOpen: Connection is already open!");
        else {
            moClientURI = new MongoClientURI(getURI());
            moClient = new MongoClient(moClientURI);
            moDB = moClient.getDB(moClientURI.getDatabase());
            //moClient.setWriteConcern(WriteConcern.majorityWriteConcern(5000, true, true));
            gridfs = new GridFS(moDB, moClientURI.getCollection());
            isConnOpen = true;
        }
    }
    
    public void Close() throws Exception{
        try {
            if (this.isOpen()){
                moClient.close();
                isConnOpen = false;
            }
            else
                throw new Exception("Close: Connection is already closed!");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
    
    public OperationResult FindOne(String key, Object value) throws Exception {
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //if (db.isAuthenticated()) {
            if (true) {
                //GridFS gridfs = new GridFS(moDB, moClientURI.getCollection());
                moDB.requestStart();
                moDB.requestEnsureConnection();
                GridFSDBFile gdbf;
                gdbf = gridfs.findOne(new BasicDBObject(key, value));
                
                CommandResult cr = moDB.getLastError();
                
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = cr.getErrorMessage() != null;
                    or.ErrorMessage = cr.getErrorMessage();
                    or.hasResult = gdbf != null;
                    if (or.hasError == false && or.hasResult) {                        
                        or._id = cr.getObjectId("_id");
                        or.GridfsFile = gdbf;
                    }
                }
                moDB.requestDone();                
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");
        return or;
    }
    
    public OperationResult FindOne(Map metadata) throws Exception {
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //if (db.isAuthenticated()) {
            if (true) {
                //GridFS gridfs = new GridFS(moDB, moClientURI.getCollection());
                moDB.requestStart();
                moDB.requestEnsureConnection();
                GridFSDBFile gdbf;
                gdbf = gridfs.findOne(new BasicDBObject(metadata));
                
                CommandResult cr = moDB.getLastError();
                
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = cr.getErrorMessage() != null;
                    or.ErrorMessage = cr.getErrorMessage();
                    or.hasResult = gdbf != null;
                    if (or.hasError == false && or.hasResult) {                        
                        or._id = cr.getObjectId("_id");
                        or.GridfsFile = gdbf;
                    }
                }
                moDB.requestDone();                
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");        
        
        return or;
    }
        
    public OperationResult Find(String key, Object value) throws Exception{
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //if (db.isAuthenticated()) {
            if (true) {
                //GridFS gridfs = new GridFS(moDB, moClientURI.getCollection());
                moDB.requestStart();
                moDB.requestEnsureConnection();
                List<GridFSDBFile> gdbf;
                gdbf = gridfs.find(new BasicDBObject(key, value));
                
                CommandResult cr = moDB.getLastError();
                
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = (cr.getErrorMessage() != null);
                    or.ErrorMessage = cr.getErrorMessage();
                    or.hasResult = (gdbf != null);
                    if (or.hasError == false && or.hasResult) {
                        or._id = cr.getObjectId("_id");
                        or.GridfsFileList = gdbf;
                    }
                }
                moDB.requestDone();
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");
        return or;
    }
    
    public OperationResult Find(Map metadata) throws Exception{
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //DB moDB = moClient.getDB(moClientURI.getDatabase());            
            //if (db.isAuthenticated()) {
            if (true) {
                //GridFS gridfs = new GridFS(moDB, moClientURI.getCollection());
                moDB.requestStart();
                moDB.requestEnsureConnection();
                List<GridFSDBFile> gdbf;
                gdbf = gridfs.find(new BasicDBObject(metadata));
                
                CommandResult cr = moDB.getLastError();
                
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = (cr.getErrorMessage() != null);
                    or.ErrorMessage = cr.getErrorMessage();
                    or.hasResult = (gdbf != null);
                    if (or.hasError == false && or.hasResult) {
                        or._id = cr.getObjectId("_id");
                        or.GridfsFileList = gdbf;
                    }
                }
                moDB.requestDone();                
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");        
        
        return or;
    }
            
    public OperationResult Save(InputStream ins, String filename) throws Exception {
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //if (db.isAuthenticated()){
            if  (true) {
                //GridFS gridfs = new GridFS(db, moClientURI.getCollection());
                //GridFS gridfs = new GridFS(moDB);
                moDB.requestStart();
                moDB.requestEnsureConnection();
                GridFSInputFile gif = gridfs.createFile(ins, filename, true);                 
                gif.save();           
                CommandResult cr = moDB.getLastError();
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = (cr.getErrorMessage() != null);
                    or.ErrorMessage = cr.getErrorMessage();
                    if (or.hasError == false) {
                        or._id = cr.getObjectId("_id");
                    } 
                }                
                moDB.requestDone();  
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");         
        
        return or;        
    }
    
    public OperationResult Save(InputStream ins, Map metadata) throws Exception {
        OperationResult or = new OperationResult();
        
        if (autoConnectionOpen) {
            if (!isConnOpen) {
                this.Open();
            }
        }
        if (isConnOpen) {
            //DB moDB = moClient.getDB(moClientURI.getDatabase());                         
            //if (db.isAuthenticated()){
            if  (true) {
                //GridFS gridfs = new GridFS(moDB, moClientURI.getCollection());
                moDB.requestStart();
                moDB.requestEnsureConnection();
                GridFSInputFile gif = gridfs.createFile(ins, "", true);
                for (Object key: metadata.keySet()) {
                    gif.put(key.toString(), metadata.get(key));
                }
                gif.save();
                CommandResult cr = moDB.getLastError();
                if (cr.ok()) {
                    or.Ok = true;
                    or.hasError = cr.getErrorMessage() != null;
                    or.ErrorMessage = cr.getErrorMessage();
                }
                moDB.requestDone();                
            }
            else throw new Exception("Find: Authentication failed!");
        }else
            throw new Exception("Find: Connection is not open!");         
        
        return or;        
    }

}
