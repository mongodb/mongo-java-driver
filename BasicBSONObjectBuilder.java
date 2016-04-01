
package com.mongodb;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import org.bson.BasicBSONObject;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
/*
    A builder for creating BasicBSONObject models from scratch.
    Utility for building complex BasicBSONObject.
    This builder and provides methods to add name/value pairs to the object model and to return the resulting object.
    The methods in this class can be chained to add multiple name/value pairs to the object.
	This class does not allow null to be used as a name(key/field) while building the BasicBSON object
    Example Code:
    # BasicBSONObjectBuilder.getInstance().add(key,value).add(key,mapValue).build()    
    # BasicBSONObject  bObject =  BasicBSONObjectBuilder.createObject(map)
*/
@SuppressWarnings("rawtypes")
public class BasicBSONObjectBuilder {
    
    private final BasicBSONObject _basicBSONObject;
    /**
     * Creates a builder intialized with an empty document.
     * @return The new empty builder
     */
    public static BasicBSONObjectBuilder getInstance() {
        return new BasicBSONObjectBuilder();
    } 
    
    /**
     * Creates a builder initialized with an empty document.
     */
    private BasicBSONObjectBuilder(){
       _basicBSONObject = new BasicBSONObject(); 
    }
        
    /**
     *  add default _id field with value document.
     * @return BasicBSONObjectBuilder
     */
    public BasicBSONObjectBuilder addId(){
        _basicBSONObject.append("_id", new ObjectId());
        return this;
    }
    
    /**
     * Add a field/element to the Object
     *
     * @param key The field name
     * @param value
     * @return the new BasicBSONObjectBuilder
     */
    public BasicBSONObjectBuilder add(final String key, final Object value){
        validateKeyField(key);        
        _basicBSONObject.append(key, value);
        return this;
    }
    
     /**
     * add a field/element with null value
     *
     * @param key The field name
     * @return the new BasicBSONObjectBuilder
     */
    public BasicBSONObjectBuilder addNull(final String key){
        validateKeyField(key);        
        _basicBSONObject.append(key, null);
        return this;
    }
    
    /**
     * Add a ObjectId type field/element 
     * @param key
     * @param value
     * @return BasicBSONObjectBuilder
     */
    
    public BasicBSONObjectBuilder addObjectId(final String key, final String value){
       validateKeyField(key);
        _basicBSONObject.append(key, new ObjectId(value));
        return this;        
    }
    
     /**
     * add a fields/elements from a Map 
     *
     * @param documentAsMap a document in Map form.
     * @return the active BasicBSONObject document
     */
    
    public  BasicBSONObjectBuilder addMap(final Map documentAsMap){
      validateMapDocument(documentAsMap);   
      _basicBSONObject.putAll(documentAsMap);
      return this;
    }
    /**
     * add a basicBSONObject Type field/element 
     * @param field
     * @param key
     * @param val
     * @return BasicBSONObjectBuilder
     */  
    public BasicBSONObjectBuilder addObject(final String field, final String key, final Object val){
        validateKeyField(field);
        validateKeyField(key);
        _basicBSONObject.append(field, BasicBSONObjectBuilder.createObject(key, val));
        return this;
    }
    
    /**
     * create BasicBSONOBject type object from the key/value 
     * @param key
     * @param val
     * @return BasicBSONObject
     */    
    public  static BasicBSONObject createObject(final String key, final Object val){
        validateKeyField(key);
        return new BasicBSONObject(key,val);       
    }
    
    /**
     * create BasicBSONOBject type object from a map
     * @param documentAsMap a document in Map form.
     * @return the new BasicBSONObject
     */
    public  static BasicBSONObject createObject(final Map documentAsMap){
      validateMapDocument(documentAsMap);      
      return new BasicBSONObject(documentAsMap);       
    }
    
     /**
     * Create BasicBSONList from the key/value 
     * @param mapList : List<Map<String, Object>>
     * @return BasicBSONList
     */
    public static BasicBSONList createNestedList(List<Map<String, Object>> mapList){
       BasicBSONList bsonList = new BasicBSONList();
       for (Map<String, Object> map : mapList) {
              bsonList.add(createObject(map));
        }
       return bsonList;
    }
    /**
     * create a list from a list of strings array
     * @param stringList
     * @return BasicBSONList
     */
    public static BasicBSONList createList(List<String> stringList){
        validateStringList(stringList);
        BasicBSONList bsonList = new BasicBSONList();
        int index = 0;
        for(String item :stringList)
        {
            if(item != null && !item.isEmpty()){
                bsonList.put( index, item);
                index++;
            }
        }              
        return bsonList;         
    }      
    /**
     * Gets the top level document.
     * @return BasicBSONObject : The base object
     */    
    public BasicBSONObject build(){        
        return _basicBSONObject;   
    }
    /**
     * create and build a single element BasicBSONObject
     * @param key
     * @param value
     * @return BasicBSONObject
     */
    public static BasicBSONObject single(final String key, Object value){       
        validateKeyField(key);
        return getInstance().add(key, value).build();
    }
    
    private static void validateMapDocument(Map aMap){
      if(aMap == null)
        throw new NullPointerException("documentAsMap value is null.");  

      if(aMap.isEmpty())
        throw new InvalidParameterException("documentAsMap value is empty.");  
            
      for(Object key: aMap.keySet()){
        if(key ==null || key.toString().isEmpty())throw new NullPointerException("one of the map's key value is empty .");
      }
    }
    private static void validateKeyField(final String key){
        if(key == null )throw new NullPointerException("key parameter value is null.");
        if( key.isEmpty())throw new InvalidParameterException("key parameter value is empty.");
    }
    private static void validateStringList(List<String> stringList){
        if(stringList == null)throw new NullPointerException("the list in null!");
        if(stringList.isEmpty())throw new InvalidParameterException("the list is empty!");
    }
    /**
     * 
     * @return int size of the top level document.
     */
    public int size(){
        return _basicBSONObject.size();
    }
     /**
     * Returns true if no key/value was inserted into the top level document.
     *
     * @return true if empty
     */
    public boolean isNullOrEmpty(){
        return ( _basicBSONObject == null || _basicBSONObject.isEmpty());
    }       
}
