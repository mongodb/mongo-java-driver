package com.mongodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.mongodb.DBApiLayer.MyCollection;

public class DBApiTranslatedCollection extends DBApiLayer.MyCollection{

	public ObjKeyTranslator getObjKeyTranslator() {
		return mObjKeyTranslator;
	}
	private ObjKeyTranslator mObjKeyTranslator;
	private DBApiLayer mDbApiLayer;

	public class ObjKeyTranslator{

		public ObjKeyTranslator(String collection) {
			mKeymapColl = mDbApiLayer.new MyCollection(collection+".keymap");
			
			
			/**Unique index: we need this to get an exception if we have concurrent access to 
			 * the collection and 2 clients add new fields.
			 */
			mKeymapColl.ensureIndex(new BasicDBObject("shortkey", 1), null, true);
			
			DBCursor cursor = mKeymapColl.find();
			int maxKey=0;
			while (cursor.hasNext()) {
				DBObject object = cursor.next();
				Object shortkey = object.get("shortkey");
				if (shortkey!=null){
					int sk = Integer.parseInt(shortkey.toString());
					if (sk>maxKey)
						maxKey=sk;
					final String longkey = object.get("_id").toString();
					mReal2shortMap.put(longkey,shortkey.toString());
					mShort2realMap.put(shortkey.toString(),longkey);
				}
			}
			mNextIndex=maxKey+1;
		}

		public DBObject toCompressed(DBObject dbObject) {
			Set<String> keySet = dbObject.keySet();
			BasicDBObject basicDBObject = new BasicDBObject();
			for (String key : keySet) {
				Object val = dbObject.get(key);
				if (val instanceof DBObject)
					val=toCompressed((DBObject) val);
				String newkey=real2short(key);
				basicDBObject.append(newkey, val);
			}
			return basicDBObject;
		}

		private HashMap<String,String> mReal2shortMap=new HashMap<>();
		private HashMap<String,String> mShort2realMap=new HashMap<>();

		private String real2short(String key) {
			String shortkey = mReal2shortMap.get(key);
			if (shortkey==null){
				while (true) {
					try {
						shortkey = "" + mNextIndex;
						mKeymapColl.insert(BasicDBObjectBuilder
								.start("_id", key).add("shortkey", shortkey)
								.get());
						mNextIndex++;
						break;
					} catch (MongoException.DuplicateKey e) {
						//continue the search for a new shortkey!
					}
					mNextIndex++;
				}
				mReal2shortMap.put(key, shortkey);
				mShort2realMap.put(shortkey,key);
			}
			return shortkey;
		}
		private int mNextIndex=0;
		private MyCollection mKeymapColl;
		/**
		 * Changes all keys to corresponding short ones.
		 * @param obj
		 * @return
		 */
		public DBObject fromCompressed(DBObject obj) {
				Set<String> keySet = obj.keySet();
				BasicDBObject basicDBObject = new BasicDBObject();
				for (String shortkey : keySet) {
					if (shortkey.equals("_id"))
						continue;
					Object val = obj.get(shortkey);
					
					String longkey=mShort2realMap.get(shortkey);
					assert(longkey!=null);
					if (val instanceof DBObject){
						//recurse...
						val=fromCompressed((DBObject) val);
					}
					basicDBObject.append(longkey, val);
				}
				return basicDBObject;
			}
		}
	
	DBApiTranslatedCollection(DBApiLayer dbApiLayer, String name) {
		dbApiLayer.super(name);
		mDbApiLayer=dbApiLayer;
		mObjKeyTranslator = new ObjKeyTranslator(name);
	}

	@Override
	protected WriteResult insert(List<DBObject> list, boolean shouldApply , com.mongodb.WriteConcern concern, DBEncoder encoder ){

		List<DBObject> listTrans=new ArrayList<>();
		for (DBObject dbObject : list) {
			DBObject newobj=mObjKeyTranslator.toCompressed(dbObject);
			listTrans.add(newobj);
		}
		return super.insert(listTrans, shouldApply, concern, encoder);
	}

	class TranslatedResult implements Iterator<DBObject>{

		private Iterator<DBObject> mResult;

		public TranslatedResult(Iterator<DBObject> result) {
			mResult=result;
		}

		@Override
		public boolean hasNext() {
			return mResult.hasNext();
		}

		@Override
		public DBObject next() {
			return mObjKeyTranslator.fromCompressed(mResult.next());
		}

		@Override
		public void remove() {
			mResult.remove();
		}
		
	}
    @Override
    Iterator<DBObject> __find( DBObject ref , DBObject fields , int numToSkip , int batchSize , int limit, int options,
                                        ReadPreference readPref, DBDecoder decoder, DBEncoder encoder )
    	{
    	DBObject queryTrans = ref ==null ? null :mObjKeyTranslator.toCompressed(ref);
    	DBObject fieldsTrans = fields==null? null : mObjKeyTranslator.toCompressed(fields);
    	Iterator<DBObject> result = super.__find(queryTrans, fieldsTrans, numToSkip, batchSize, limit, options, readPref, decoder, encoder);
    	return new TranslatedResult(result);
    	}
    @Override
    public WriteResult remove(DBObject o, WriteConcern concern,
    		DBEncoder encoder) {
    	if (o!=null)
    		o = mObjKeyTranslator.toCompressed(o);
    	return super.remove(o, concern, encoder);
    }
    
    @Override
    public WriteResult update( DBObject query , DBObject o , boolean upsert , boolean multi , com.mongodb.WriteConcern concern, DBEncoder encoder ){
    	if (query!=null){
    		query=mObjKeyTranslator.toCompressed(query);    		
    	}
    	if (o!=null){
    		o=mObjKeyTranslator.toCompressed(o);
    	}
    		
    	return super.update(query, o, upsert, multi, concern, encoder);
    }
}
