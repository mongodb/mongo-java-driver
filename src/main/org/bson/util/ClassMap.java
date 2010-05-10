// ClassMap.java

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

package org.bson.util;

import java.util.*;

/**
 * Maps Class objects to values. A ClassMap is different from a regular Map 
 * in that get(c) does not only look to see if 'c' is a key in the 
 * Map, but also walks the up superclass and interface graph of 'c' 
 * to find matches. Derived matches of this sort are then "cached" in the 
 * registry so that matches are faster on future gets. 
 * 
 * This is a very useful class for Class based registries. 
 * 
 * Example: 
 *
 * ClassMap<String> m = new ClassMap<String>();
 * m.put(Animal.class, "Animal");
 * m.put(Fox.class, "Fox");
 * System.out.println(m.get(Fox.class)) // --> "Fox"
 * System.out.println(m.get(Dog.class)) // --> "Animal"
 *
 * (assuming Dog.class &lt; Animal.class)
 * 
 */
public class ClassMap<T> implements Map <Class, T> {

    /**
     * internalMap
     */
    protected Map<Class, T> getInternalMap(){
        return(_internalMap);
    }

    private void setInternalMap(Map m){
        _internalMap = m;
    }

    /**
     * cache
     */
    protected Map<Class, T> getCache(){
        return(_cache);
    }
    
    private void setCache(Map m){
	_cache = m;
    }
    
    /**
     * size
     */
    public synchronized int size(){
	return(getCache().size());
    }
    
    /**
     * isEmpty
     */
    public synchronized boolean isEmpty(){
        return(getCache().isEmpty()); 
    }

    /**
     * containsKey
     */
    public synchronized boolean containsKey(Object key){
        return(get(key) != null);
    }

    /**
     * cacheContainsKey
     */
    protected synchronized boolean cacheContainsKey(Object key){
        return(getCache().containsKey(key));
    }
    
    /**
     * containsValue
     */
    public synchronized boolean containsValue(Object object){
        return(getCache().containsValue(object));
    }

    /**
     * get
     */
    public synchronized T get(Object key){
        Class c = (Class)key;
	Map<Class, T> cache = getCache();
	if (cache.containsKey(c)){
	    return(cache.get(c));
	}
	
	T result = computeValue(c);
	cache.put(c, result);  // will also cache failures
	return(result);
    }
    
    /**
     * put
     */
    public synchronized T put(Class key, T value){
        T result = getInternalMap().put(key, value);
	initCache();
	return(result);
    }

    /**
     * remove
     */
    public synchronized T remove(Object object){
        T result = getInternalMap().remove(object);
	initCache();
	return(result);
    }

    /**
     * putAll
     */
    public synchronized void putAll(Map map){
        getInternalMap().putAll(map);
        initCache();
    }

    /**
     * clear
     */
    public synchronized void clear(){   
        getInternalMap().clear();
        initCache();
    }

    /**
     * keySet
     */
    public synchronized Set<Class> keySet(){           
        return(getCache().keySet());
    }

    /**
     * values
     */
    public synchronized Collection<T> values(){
        return(getCache().values());
    }

    /**
     * entrySet
     */
    public synchronized Set<Map.Entry<Class, T>> entrySet(){
        return(getCache().entrySet());
    }

    /**
     * equals
     */
    public boolean equals(Object object){
        try {
  	    ClassMap that = (ClassMap)object;
	    return(getInternalMap().equals(that.getInternalMap()));
        } catch (ClassCastException cce) {
	    return(false);
        }
    }

    /**
     * hashCode
     */
    public int hashCode(){    
        return(getInternalMap().hashCode());  
    }

    /**
     * computeValue
     */
    private T computeValue(Class key){ 
	List<Class> ancestry = getAncestry(key);
	Map<Class, T> map = getCache();
	for (Class c : ancestry) {
	    if (map.containsKey(c)) {
		T value = map.get(c);
		return(value);
	    }
	}
	return(null);
    }

    /**
     * initCache
     */
    protected void initCache(){
	Map cache = getCache();
        cache.clear();
	cache.putAll(getInternalMap());
    }

    /**
     * toString
     */
    public String toString() {
	return(getCache().toString());
    }

    /**
     * getAncestry
     *
     * Walks superclass and interface graph, superclasses first, then
     * interfaces, to compute an ancestry list. Supertypes are visited 
     * left to right. Duplicates are removed such that no Class will 
     * appear in the list before one of its subtypes. 
     * 
     * Does not need to be synchronized, races are harmless
     * as the Class graph does not change at runtime.
     */
    public static List<Class> getAncestry(Class c){
	List<Class> result = null;

	Map<Class, List<Class>> cache = getClassAncestryCache();
        List<Class> cachedResult = cache.get(c);
        if (cachedResult != null) {
	    result = cachedResult;
	} else {
	    result = new ArrayList<Class>();

	    List<Class> ancestry = computeAncestry(c);
	    int size = ancestry.size();
	    for (int i = 0; i < size; i++) {
		result.add(ancestry.get((size - i) - 1));
	    }
	    
	    cache.put(c, result);
	}

        return(result);
    }

    /**
     * computeAncestry
     */
    private static List<Class> computeAncestry(Class c){
	List<Class> result = new ArrayList<Class>();
	result.add(Object.class);
	computeAncestry(c, result);
	return(result);
    }

    private static void computeAncestry(Class c, List result){
        if ((c == null) || (c == Object.class)){  
	    return;
	}

	// first interfaces (looks backwards but is not)
	Class[] interfaces = c.getInterfaces();
	for (int i = interfaces.length - 1; i >= 0; i--){ 
	    computeAncestry(interfaces[i], result);
	}
	
	// next superclass
	computeAncestry(c.getSuperclass(), result);
	
	if (!result.contains(c))
	    result.add(c);
    }

    /**
     * classAncestryCache
     */
    private static Map getClassAncestryCache(){
	return(_ancestryCache);
    }

    private static void setClassAncestryCache(Map m){
	_ancestryCache = m;
    }

    /**
     * private members
     */
    private Map<Class, T> _internalMap = new HashMap<Class, T>();

    private Map<Class, T> _cache = new HashMap<Class, T>();

    private static Map<Class, List<Class>> _ancestryCache = 
	new HashMap<Class, List<Class>>();

}
