/**
 * 
 */
package com.google.code.morphia.mapping.lazy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import junit.framework.Assert;

import org.junit.Ignore;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.mapping.lazy.proxy.ProxiedReference;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 *
 */
@Ignore
@SuppressWarnings("unchecked")
public class ProxyTestBase extends TestBase {

	protected void assertFetched(final Object e) {
		Assert.assertTrue(isFetched(e));
	}

	protected void assertNotFetched(final Object e) {
		Assert.assertFalse(isFetched(e));
	}

	protected boolean isFetched(final Object e) {
		return asProxiedReference(e).__isFetched();
	}

	protected ProxiedReference asProxiedReference(final Object e) {
		return (ProxiedReference) e;
	}

	protected void assertIsProxy(Object p) {
		Assert.assertTrue(p instanceof ProxiedReference);
	}

	protected <T> T deserialize(final Object t) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream os = new ObjectOutputStream(baos);
			os.writeObject(t);
			os.close();
			byte[] ba = baos.toByteArray();

			return (T) new ObjectInputStream(new ByteArrayInputStream(ba))
			.readObject();
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}


}
