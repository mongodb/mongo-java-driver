/**
 * 
 */
package com.google.code.morphia.mapping.lazy.proxy;

import java.io.IOException;
import java.io.Serializable;

import com.google.code.morphia.Key;
import com.google.code.morphia.mapping.lazy.DatastoreProvider;
import com.thoughtworks.proxy.kit.ObjectReference;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 * 
 */
@SuppressWarnings({"unchecked","rawtypes"})
public abstract class AbstractReference implements Serializable, ObjectReference, ProxiedReference {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected final DatastoreProvider p;
	protected final boolean ignoreMissing;
	protected Object object;
	private boolean isFetched = false;
	protected final Class referenceObjClass;

	protected AbstractReference(final DatastoreProvider p,
			final Class referenceObjClass, final boolean ignoreMissing) {
		this.p = p;
		this.referenceObjClass = referenceObjClass;
		this.ignoreMissing = ignoreMissing;
	}

	public final synchronized Object get() {
		if (isFetched) {
			return object;
		}

		object = fetch();
		isFetched = true;
		return object;
	}

	protected abstract Object fetch();

	public final void set(final Object arg0) {
		throw new UnsupportedOperationException();
	}

	public final boolean __isFetched() {
		return isFetched;
	}

	protected final Object fetch(Key<?> id) {
		return p.get().getByKey(referenceObjClass, id);
	}


	private void writeObject(final java.io.ObjectOutputStream out)
	throws IOException {
		// excessive hoop-jumping in order not to have to recreate the
		// instance.
		// as soon as weÂ´d have an ObjectFactory, that would be unnecessary
		beforeWriteObject();
		isFetched = false;
		out.defaultWriteObject();
	}

	protected void beforeWriteObject() {
	}

	public final Class __getReferenceObjClass() {
		return referenceObjClass;
	}

	public Object __unwrap() {
		return get();
	}
}
