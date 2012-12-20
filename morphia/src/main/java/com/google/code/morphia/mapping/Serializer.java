/**
 * 
 */
package com.google.code.morphia.mapping;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.bson.types.Binary;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class Serializer
{
	/** serializes object to byte[] */
	public static byte[] serialize(final Object o, final boolean zip) throws IOException
	{
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputStream os = baos;
		if (zip)
		{
			os = new GZIPOutputStream(os);
		}
		final ObjectOutputStream oos = new ObjectOutputStream(os);
		oos.writeObject(o);
		oos.flush();
		oos.close();
		
		return baos.toByteArray();
	}
	
	/** deserializes DBBinary/byte[] to object */
	public static Object deserialize(final Object data, final boolean zipped) throws IOException,
	ClassNotFoundException
	{
		ByteArrayInputStream bais;
		if (data instanceof Binary)
		{
			bais = new ByteArrayInputStream(((Binary) data).getData());
		}
		else
		{
			bais = new ByteArrayInputStream((byte[]) data);
		}
		
		InputStream is = bais;
		try
		{
			if (zipped)
			{
				is = new GZIPInputStream(is);
			}
			
			final ObjectInputStream ois = new ObjectInputStream(is);
			return ois.readObject();
		}
		finally
		{
			is.close();
		}
	}
	
}
