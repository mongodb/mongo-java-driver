// GridFSInputFile.java

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

package com.mongodb.gridfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.Date;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.util.SimplePool;
import com.mongodb.util.Util;

/**
 * Class implementation for writing data to GridFS.
 * 
 * @author Eliot Horowitz and Guy K. Kloss
 */
public class GridFSInputFile extends GridFSFile {
	
	/**
	 * Default constructor setting the GridFS file name and providing an input
	 * stream containing data to be written to the file.
	 * 
	 * @param fs
	 *            The GridFS connection handle.
	 * @param in
	 *            Stream used for reading data from.
	 * @param filename
	 *            Name of the file to be created.
	 */
	GridFSInputFile(GridFS fs , InputStream in , String filename) {
		_fs = fs;
		_in = in;
		_filename = filename;
		
		_id = new ObjectId();
		_chunkSize = GridFS.DEFAULT_CHUNKSIZE;
		_uploadDate = new Date();
		_messageDigester = _md5Pool.get();
		_messageDigester.reset();
		_buffer = new byte[(int) _chunkSize];
	}
	
	/**
	 * Constructor that only provides a file name, but does not rely on the
	 * presence of an {@link java.io.InputStream}. An
	 * {@link java.io.OutputStream} can later be obtained for writing using the
	 * {@link #getOutputStream()} method.
	 * 
	 * @param fs
	 *            The GridFS connection handle.
	 * @param filename
	 *            Name of the file to be created.
	 */
	GridFSInputFile(GridFS fs , String filename) {
		this( fs , null , filename );
	}
	
	/**
	 * Minimal constructor that does not rely on the presence of an
	 * {@link java.io.InputStream}. An {@link java.io.OutputStream} can later be
	 * obtained for writing using the {@link #getOutputStream()} method.
	 * 
	 * @param fs
	 *            The GridFS connection handle.
	 */
	GridFSInputFile(GridFS fs) {
		this( fs , null , null );
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @see com.mongodb.gridfs.GridFSFile#getMetaData()
	 */
	public DBObject getMetaData() {
		if (_metadata == null) {
			_metadata = new BasicDBObject();
		}
		return _metadata;
	}
	
	/**
	 * Sets the file name on the GridFS entry.
	 * 
	 * @param fn
	 *            File name.
	 */
	public void setFilename(String fn) {
		_filename = fn;
	}
	
	/**
	 * Sets the content type (MIME type) on the GridFS entry.
	 * 
	 * @param ct
	 *            Content type.
	 */
	public void setContentType(String ct) {
		_contentType = ct;
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * @see com.mongodb.gridfs.GridFSFile#save()
	 */
	public void save() {
		save( GridFS.DEFAULT_CHUNKSIZE );
	}
	
	/**
	 * Saves the new GridFS entry with a non-default chunk size.
	 * 
	 * @param chunkSize
	 *            Size of chunks for file in bytes.
	 */
	public void save(int chunkSize) {
		if (!_saved) {
			try {
				saveChunks( chunkSize );
			} catch (IOException ioe) {
				throw new MongoException( "couldn't save chunks" , ioe );
			}
		}
		
		if (_outputStream == null) {
			_close();
		}
	}
	
	/**
	 * Saves all data from configured {@link java.io.InputStream} input stream
	 * to GridFS. If an {@link java.io.OutputStream} has been obtained (with the
	 * {@link #getOutputStream()} method), the last, partial chunk is not
	 * written. It is written upon the call to
	 * {@link java.io.OutputStream#close()} on that stream.
	 * 
	 * @return Number of the next chunk.
	 * @throws IOException
	 *             on problems reading the new entry's
	 *             {@link java.io.InputStream}.
	 */
	public int saveChunks() throws IOException {
		return saveChunks( GridFS.DEFAULT_CHUNKSIZE );
	}
	
	/**
	 * Saves all data from configured {@link java.io.InputStream} input stream
	 * to GridFS. For writing a non-default chunk size is used. If an
	 * {@link java.io.OutputStream} has been obtained (with the
	 * {@link #getOutputStream()} method), the last, partial chunk is not
	 * written. It is written upon the call to
	 * {@link java.io.OutputStream#close()} on that stream.
	 * 
	 * @param chunkSize
	 *            Size of chunks for file in bytes.
	 * @return Number of the next chunk.
	 * @throws IOException
	 *             on problems reading the new entry's
	 *             {@link java.io.InputStream}.
	 */
	public int saveChunks(int chunkSize) throws IOException {
		if (_chunkSize != chunkSize) {
			_chunkSize = chunkSize;
			_buffer = new byte[(int) _chunkSize];
		}
		if (_saved) {
			throw new RuntimeException( "already saved!" );
		}
		
		if (chunkSize > 3.5 * 1000 * 1000) {
			throw new RuntimeException( "chunkSize must be less than 3.5MiB!" );
		}
		
		int bytesRead = 0;
		while (bytesRead >= 0) {
			_currentBufferPosition = 0;
			bytesRead = _readStream2Buffer();
			_dumpBuffer( _outputStream == null );
		}
		
		if (_outputStream == null) {
			_close();
		}
		return _currentChunkNumber;
	}
	
	/**
	 * After retrieving this {@link java.io.OutputStream}, this object will be
	 * capable of accepting successively written data to the output stream. All
	 * operations proceed as usual, only the {@link #save()}, {@link #save(int)}
	 * , {@link #saveChunks()} and {@link #saveChunks(int)} methods will
	 * <b>not</b> finalise the and close writing the GridFS file. They will
	 * <b>only</b> read out the potentially used {@link java.io.InputStream} and
	 * flush it to the internal write buffer. To completely persist this GridFS
	 * object, you must finally call the {@link java.io.OutputStream#close()}
	 * method on the output stream.
	 * 
	 * @return Writable stream object.
	 */
	public OutputStream getOutputStream() {
		if (_outputStream == null) {
			_outputStream = new MyOutputStream();
		}
		return _outputStream;
	}
	
	/**
	 * Dumps a new chunk into the chunks collection. Depending on the flag, also
	 * partial buffers (at the end) are going to be written immediately.
	 * 
	 * @param data
	 *            Data for chunk.
	 * @param writePartial
	 *            Write also partial buffers full.
	 */
	private void _dumpBuffer(boolean writePartial) {
		if ((_currentBufferPosition < _chunkSize) && !writePartial) {
			// Bail out, nothing to write (yet).
			return;
		}
		byte[] writeBuffer = _buffer;
		if (_currentBufferPosition != _chunkSize) {
			writeBuffer = new byte[_currentBufferPosition];
			System.arraycopy( _buffer, 0, writeBuffer, 0, _currentBufferPosition );
		}
		
		DBObject chunk = BasicDBObjectBuilder.start()
				.add( "files_id", _id )
				.add( "n", _currentChunkNumber )
				.add( "data", writeBuffer ).get();
		_fs._chunkCollection.save( chunk );
		_currentChunkNumber++;
		_totalBytes += writeBuffer.length;
		_messageDigester.update( writeBuffer );
		_currentBufferPosition = 0;
	}
	
	/**
	 * Reads a buffer full from the {@link java.io.InputStream}.
	 * 
	 * @return Number of bytes read from stream.
	 * @throws IOException
	 *             if the reading from the stream fails.
	 */
	private int _readStream2Buffer() throws IOException {
		int bytesRead = 0;
		while (_currentBufferPosition < _chunkSize && bytesRead >= 0) {
			bytesRead = _in.read( _buffer, _currentBufferPosition,
					             (int) _chunkSize - _currentBufferPosition );
			if (bytesRead > 0) {
				_currentBufferPosition += bytesRead;
			} else if (bytesRead == 0) {
				throw new RuntimeException( "i'm doing something wrong" );
			}
		}
		return bytesRead;
	}
	
	/**
	 * Persist the GridFS object by writing finally also the object in the file
	 * collection. Calls the super class save() method.
	 */
	private void _close() {
		if (!_saved) {
			_md5 = Util.toHex( _messageDigester.digest() );
			_md5Pool.done( _messageDigester );
			_length = _totalBytes;
			_saved = true;
		}
		super.save();
	}
	
	private final InputStream _in;
	private boolean _saved = false;
	private byte[] _buffer = null;
	private int _currentChunkNumber = 0;
	private int _currentBufferPosition = 0;
	private long _totalBytes = 0;
	private MessageDigest _messageDigester = null;
	private OutputStream _outputStream = null;
	
	/**
	 * A pool of {@link java.security.MessageDigest} objects.
	 */
	static SimplePool<MessageDigest> _md5Pool
			= new SimplePool<MessageDigest>( "md5" , 10 , -1 , false , false ) {
		/**
		 * {@inheritDoc}
		 * 
		 * @see com.mongodb.util.SimplePool#createNew()
		 */
		protected MessageDigest createNew() {
			try {
				return MessageDigest.getInstance( "MD5" );
			} catch (java.security.NoSuchAlgorithmException e) {
				throw new RuntimeException( "your system doesn't have md5!" );
			}
		}
	};
	
	/**
	 * An output stream implementation that can be used to successively write to
	 * a GridFS file.
	 * 
	 * @author Guy K. Kloss
	 */
	class MyOutputStream extends OutputStream {
		
		/**
		 * {@inheritDoc}
		 * 
		 * @see java.io.OutputStream#write(int)
		 */
		@Override
		public void write(int b) throws IOException {
			byte[] byteArray = new byte[1];
			byteArray[0] = (byte) (b & 0xff);
			write( byteArray, 0, 1 );
		}
		
		/**
		 * {@inheritDoc}
		 * 
		 * @see java.io.OutputStream#write(byte[], int, int)
		 */
		@Override
		public void write(byte[] b , int off , int len) throws IOException {
			int offset = off;
			int length = len;
			int toCopy = 0;
			while (length > 0) {
				toCopy = length;
				if (toCopy > _chunkSize - _currentBufferPosition) {
					toCopy = (int) _chunkSize - _currentBufferPosition;
				}
				System.arraycopy( b, offset, _buffer, _currentBufferPosition, toCopy );
				_currentBufferPosition += toCopy;
				offset += toCopy;
				length -= toCopy;
				if (_currentBufferPosition == _chunkSize) {
					_dumpBuffer( false );
				}
			}
		}
		
		/**
		 * Processes/saves all data from {@link java.io.InputStream} and closes
		 * the potentially present {@link java.io.OutputStream}. The GridFS file
		 * will be persisted afterwards.
		 */
		@Override
		public void close() {
			_dumpBuffer( true );
			_close();
		}
	}
}
