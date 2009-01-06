// DBCursor.java

package ed.db;

import java.util.*;

import ed.util.*;

/**
 */
public class DBCursor {

    /** Initializes a new database cursor
     * @param collection collection to use
     * @param q query to perform
     * @param k keys to return from the query
     * @param cons the constructor for the collection
     */
    public DBCursor( DBCollection collection , DBObject q , DBObject k ){
        _collection = collection;
        _query = q;
        _keysWanted = k;
    }

    /** Types of cursors: iterator or array. */
    static enum CursorType { ITERATOR , ARRAY };

    public DBCursor copy(){
	DBCursor c = new DBCursor( _collection , _query , _keysWanted );
	c._orderBy = _orderBy;
        c._hint = _hint;
	c._numWanted = _numWanted;
	c._skip = _skip;
	return c;
    }

    // ---- querty modifiers --------

    /** Sorts this cursor's elements.
     * @param orderBy the fields on which to sort
     * @return a cursor pointing to the first element of the sorted results
     * @throws RuntimeException if the iterator exists
     */
    public DBCursor sort( DBObject orderBy ){
        if ( _it != null )
            throw new RuntimeException( "can't sort after executing query" );

        _orderBy = orderBy;
        return this;
    }

    public DBCursor hint( DBObject indexKeys ){
        if ( _it != null )
            throw new RuntimeException( "can't hint after executing query" );
        
        if ( indexKeys == null )
            _hint = null;
        else 
            _hint = DBCollection.genIndexName( indexKeys );
        return this;
    }

    public DBCursor hint( String indexName ){
        if ( _it != null )
            throw new RuntimeException( "can't hint after executing query" );

        _hint = indexName;
        return this;
    }

    public DBObject explain(){
        DBCursor c = copy();
        c._explain = true;
        return c.next();
    }

    /** Limits the number of elements returned.
     * @param n the number of elements to return
     * @return a cursor pointing to the first element of the limited results
     * @throws RuntimeException if the iterator exists
     */
    public DBCursor limit( int n ){
        if ( _it != null )
            throw new RuntimeException( "can't set limit after executing query" );

        _numWanted = n;
        return this;
    }

    /** Discards a given number of elements at the beginning of the cursor.
     * @param n the number of elements to skip
     * @return a cursor pointing to the new first element of the results
     * @throws RuntimeException if the iterator exists
     */
    public DBCursor skip( int n ){
        if ( _it != null )
            throw new RuntimeException( "can't set skip after executing query" );
        _skip = n;
        return this;
    }

    // ----  internal stuff ------

    private void _check(){
        if ( _it != null )
            return;
        
        if ( _collection != null && _query != null ){

            _lookForHints();

            DBObject foo = _query;
            if ( hasSpecialQueryFields() ){
                foo = new BasicDBObject();
                _addToQueryObject( foo , "query" , _query , true );
                _addToQueryObject( foo , "orderby" , _orderBy , false );
                _addToQueryObject( foo , "$hint" , _hint );
                if ( _explain )
                    foo.put( "$explain" , true );
            }

            final long start = System.currentTimeMillis();
            _it = _collection.find( foo , _keysWanted , _skip , -1 * _numWanted );
        }

        if ( _it == null )
            _it = (new LinkedList<DBObject>()).iterator();
    }
    
    /**
     * if there is a hint to use, use it
     */
    private void _lookForHints(){
        
        if ( _hint != null ) // if someone set a hint, then don't do this
            return;

        if ( _collection._hintFields == null )
            return;

        Set<String> mykeys = _query.keySet();

        for ( DBObject o : _collection._hintFields ){
            
            Set<String> hintKeys = o.keySet();

            if ( ! mykeys.containsAll( hintKeys ) )
                continue;

            hint( o );
            return;
        }
    }

    boolean hasSpecialQueryFields(){
        if ( _orderBy != null && _orderBy.keySet().size() > 0 )
            return true;
        
        if ( _hint != null )
            return true;
        
        return _explain;
    }

    void _addToQueryObject( DBObject query , String field , DBObject thing , boolean sendEmpty ){
        if ( thing == null )
            return;
        
        if ( ! sendEmpty && thing.keySet().size() == 0 )
            return;
    
        _noRefCheck( thing );
    
        _addToQueryObject( query , field , thing );
    }

    void _addToQueryObject( DBObject query , String field , Object thing ){

        if ( thing == null )
            return;
        
        query.put( field , thing );
    }

    void _noRefCheck( DBObject o ){
        if ( ! Bytes.cameFromDB( o ) )
            return;
        
        o.put( Bytes.NO_REF_HACK , "z" );
    }

    /** @unexpose */
    void _checkType( CursorType type ){
        if ( _cursorType == null ){
            _cursorType = type;
            return;
        }

        if ( type == _cursorType )
            return;

        throw new RuntimeException( "can't switch cursor access methods" );
    }

    private DBObject _next(){
        if ( _cursorType == null )
            _checkType( CursorType.ITERATOR );

        _check();

        _cur = null;
        _cur = _it.next();
        _collection.apply( _cur , false );
        _num++;

        if ( _keysWanted != null && _keysWanted.keySet().size() > 0 ){
            //_cur.markAsPartialObject();
            throw new RuntimeException( "need to figure out partial" );
        }

        if ( _cursorType == CursorType.ARRAY ){
            _nums.add( String.valueOf( _all.size() ) );
            _all.add( _cur );
        }

        return _cur;
    }

    private boolean _hasNext(){
        _check();

        if ( _numWanted > 0 && _num >= _numWanted )
            return false;

        return _it.hasNext();
    }

    /** @unexpose */
    public int numSeen(){
	return _num;
    }

    // ----- iterator api -----

    /** Checks if there is another element.
     * @return if there is another element
     */
    public boolean hasNext(){
        _checkType( CursorType.ITERATOR );
        return _hasNext();
    }

    /** Returns the element the cursor is at and moves the cursor ahead by one.
     * @return the next element
     */
    public DBObject next(){
        _checkType( CursorType.ITERATOR );
        return _next();
    }

    /** Returns the element the cursor is at.
     * @return the next element
     */
    public DBObject curr(){
        _checkType( CursorType.ITERATOR );
        return _cur;
    }

    /** Unimplemented.
     * @throws RuntimeException
     */
    public void remove(){
        throw new RuntimeException( "no" );
    }


    //  ---- array api  -----

    /** @unexpose */
    void _fill( int n ){
        _checkType( CursorType.ARRAY );
        while ( n >= _all.size() && _hasNext() )
            _next();
    }

    /** Finds the number of elements in the array.
     * @return the number of elements in the array
     */
    public int length(){
        _checkType( CursorType.ARRAY );
        _fill( Integer.MAX_VALUE );
        return _all.size();
    }

    /** Converts this cursor to an array.
     * @return an array of elements
     */
    public List toArray(){
        return toArray( Integer.MAX_VALUE );
    }

    /** Converts this cursor to an array.  If there are more than a given number of elements in the resulting array, only return the first <tt>min</tt>.
     * @return min the minimum size of the array to return
     * @return an array of elements
     */
    public List toArray( int min ){
        _checkType( CursorType.ARRAY );
        _fill( min );
        return Collections.unmodifiableList( _all );
    }


    /** Counts the number of elements in this cursor.
     * @return the number of elements
     */
    public int count(){
        if ( _collection == null )
            throw new RuntimeException( "why is _collection null" );
        if ( _collection._base == null )
            throw new RuntimeException( "why is _collection._base null" );
             
        throw new RuntimeException( "count isn't implemnented yet" );
    }


    // ----  query setup ----
    final DBCollection _collection;
    final DBObject _query;
    final DBObject _keysWanted;
    
    private DBObject _orderBy;
    private String _hint;
    private boolean _explain = false;
    private int _numWanted = 0;
    private int _skip = 0;

    // ----  result info ----
    private Iterator<DBObject> _it;

    private CursorType _cursorType;
    private DBObject _cur = null;
    private int _num = 0;

    private final ArrayList<DBObject> _all = new ArrayList<DBObject>();
    private final List<String> _nums = new ArrayList<String>();

}
