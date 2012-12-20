package com.google.code.morphia;

import com.google.code.morphia.annotations.PostLoad;
import com.google.code.morphia.annotations.PostPersist;
import com.google.code.morphia.annotations.PreLoad;
import com.google.code.morphia.annotations.PrePersist;
import com.google.code.morphia.annotations.PreSave;
import com.google.code.morphia.mapping.Mapper;
import com.mongodb.DBObject;

/** Interface for intercepting @Entity lifecycle events */
public interface EntityInterceptor {
	/** see {@link PrePersist} */
	void prePersist(Object ent, DBObject dbObj, Mapper mapr);
	/** see {@link PreSave} */
	void preSave(Object ent, DBObject dbObj, Mapper mapr);
	/** see {@link PostPersist} */
	void postPersist(Object ent, DBObject dbObj, Mapper mapr);
	/** see {@link PreLoad} */
	void preLoad(Object ent, DBObject dbObj, Mapper mapr);
	/** see {@link PostLoad} */
	void postLoad(Object ent, DBObject dbObj, Mapper mapr);
}
