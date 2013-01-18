/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.query;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.DatastoreImpl;
import com.google.code.morphia.Key;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.mapping.MappedClass;
import com.google.code.morphia.mapping.MappedField;
import com.google.code.morphia.mapping.Mapper;
import com.google.code.morphia.mapping.cache.EntityCache;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;
import org.bson.BSONObject;
import org.bson.types.CodeWScope;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <p>Implementation of Query</p>
 *
 * @param <T> The type we will be querying for, and returning.
 * @author Scott Hernandez
 */
public class QueryImpl<T> extends CriteriaContainerImpl implements Query<T>, Criteria {
    private static final Logr log = MorphiaLoggerFactory.get(QueryImpl.class);

    private EntityCache cache;
    private boolean validateName = true;
    private boolean validateType = true;

    private String[] fields = null;
    private Boolean includeFields = null;
    private BasicDBObject sort = null;
    private DatastoreImpl ds = null;
    private DBCollection dbColl = null;
    private int offset = 0;
    private int limit = -1;
    private int batchSize = 0;
    private String indexHint;
    private Class<T> clazz = null;
    private BasicDBObject baseQuery = null;
    private boolean snapshotted = false;
    private boolean noTimeout = false;
    private boolean tail = false;
    private boolean tail_await_data;
    private ReadPreference readPref = null;

    public QueryImpl(final Class<T> clazz, final DBCollection coll, final Datastore ds) {
        super(CriteriaJoin.AND);

        this.query = this;
        this.clazz = clazz;
        this.ds = ((DatastoreImpl) ds);
        this.dbColl = coll;
        this.cache = this.ds.getMapper().createEntityCache();

        final MappedClass mc = this.ds.getMapper().getMappedClass(clazz);
        final Entity entAn = mc == null ? null : mc.getEntityAnnotation();
        if (entAn != null) {
            this.readPref = this.ds.getMapper().getMappedClass(clazz).getEntityAnnotation().queryNonPrimary() ?
                    ReadPreference.secondaryPreferred() : null;
        }
    }

    public QueryImpl(final Class<T> clazz, final DBCollection coll, final Datastore ds, final int offset,
                     final int limit) {
        this(clazz, coll, ds);
        this.offset = offset;
        this.limit = limit;
    }

    public QueryImpl(final Class<T> clazz, final DBCollection coll, final DatastoreImpl ds, final DBObject baseQuery) {
        this(clazz, coll, ds);
        this.baseQuery = (BasicDBObject) baseQuery;
    }

    @Override
    public QueryImpl<T> clone() {
        final QueryImpl<T> n = new QueryImpl<T>(clazz, dbColl, ds);
        n.batchSize = batchSize;
        n.cache = this.ds.getMapper().createEntityCache(); // fresh cache
        n.fields = fields == null ? null : Arrays.copyOf(fields, fields.length);
        n.includeFields = includeFields;
        n.indexHint = indexHint;
        n.limit = limit;
        n.noTimeout = noTimeout;
        n.query = n; // feels weird, correct?
        n.offset = offset;
        n.readPref = readPref;
        n.snapshotted = snapshotted;
        n.validateName = validateName;
        n.validateType = validateType;
        n.sort = (BasicDBObject) (sort == null ? null : sort.clone());
        n.baseQuery = (BasicDBObject) (baseQuery == null ? null : baseQuery.clone());

        // fields from superclass
        n.attachedTo = attachedTo;
        n.children = children == null ? null : new ArrayList<Criteria>(children);
        n.tail = tail;
        n.tail_await_data = tail_await_data;
        return n;
    }

    public DBCollection getCollection() {
        return dbColl;
    }

    public void setQueryObject(final DBObject query) {
        this.baseQuery = (BasicDBObject) query;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }

    public DBObject getQueryObject() {
        final DBObject obj = new BasicDBObject();

        if (this.baseQuery != null) {
            obj.putAll((BSONObject) this.baseQuery);
        }

        this.addTo(obj);

        return obj;
    }

    public DatastoreImpl getDatastore() {
        return ds;
    }

    public DBObject getFieldsObject() {
        if (fields == null || fields.length == 0) {
            return null;
        }

        final Map<String, Integer> fieldsFilter = new HashMap<String, Integer>();
        for (String field : this.fields) {
            final StringBuffer sb = new StringBuffer(field); //validate might modify prop string to translate java
            // field name to db field name
            Mapper.validate(clazz, ds.getMapper(), sb, FilterOperator.EQUAL, null, validateName, false);
            field = sb.toString();
            fieldsFilter.put(field, (includeFields ? 1 : 0));
        }

        //Add className field just in case.
        if (includeFields) {
            fieldsFilter.put(Mapper.CLASS_NAME_FIELDNAME, 1);
        }

        return new BasicDBObject(fieldsFilter);
    }

    public DBObject getSortObject() {
        return (sort == null) ? null : sort;
    }

    public boolean isValidatingNames() {
        return validateName;
    }

    public boolean isValidatingTypes() {
        return validateType;
    }

    public long countAll() {
        final DBObject query = getQueryObject();
        if (log.isTraceEnabled()) {
            log.trace("Executing count(" + dbColl.getName() + ") for query: " + query);
        }
        return dbColl.getCount(query);
    }

    public DBCursor prepareCursor() {
        final DBObject query = getQueryObject();
        final DBObject fields = getFieldsObject();

        if (log.isTraceEnabled()) {
            log.trace("Running query(" + dbColl.getName() + ") : " + query + ", fields:" + fields + "," +
                              "off:" + offset + ",limit:" + limit);
        }

        final DBCursor cursor = dbColl.find(query, fields);

        if (offset > 0) {
            cursor.skip(offset);
        }
        if (limit > 0) {
            cursor.limit(limit);
        }
        if (batchSize > 0) {
            cursor.batchSize(batchSize);
        }
        if (snapshotted) {
            cursor.snapshot();
        }
        if (sort != null) {
            cursor.sort(sort);
        }
        if (indexHint != null) {
            cursor.hint(indexHint);
        }

        if (null != readPref) {
            cursor.setReadPreference(readPref);
        }

        if (noTimeout) {
            cursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
        }

        if (tail) {
            cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
            if (tail_await_data) {
                cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
            }
        }

        //Check for bad options.
        if (snapshotted && (sort != null || indexHint != null)) {
            log.warning("Snapshotted query should not have hint/sort.");
        }

        if (tail && (sort != null)) {
            // i don´t think that just warning is enough here, i´d favor a RTE, agree?
            log.warning("Sorting on tail is not allowed.");
        }

        return cursor;
    }


    public Iterable<T> fetch() {
        final DBCursor cursor = prepareCursor();
        if (log.isTraceEnabled()) {
            log.trace("Getting cursor(" + dbColl.getName() + ")  for query:" + cursor.getQuery());
        }

        return new MorphiaIterator<T, T>(cursor, ds.getMapper(), clazz, dbColl.getName(), cache);
    }


    public Iterable<Key<T>> fetchKeys() {
        final String[] oldFields = fields;
        final Boolean oldInclude = includeFields;
        fields = new String[]{Mapper.ID_KEY};
        includeFields = true;
        final DBCursor cursor = prepareCursor();

        if (log.isTraceEnabled()) {
            log.trace("Getting cursor(" + dbColl.getName() + ") for query:" + cursor.getQuery());
        }

        fields = oldFields;
        includeFields = oldInclude;
        return new MorphiaKeyIterator<T>(cursor, ds.getMapper(), clazz, dbColl.getName());
    }


    @SuppressWarnings("unchecked")
    public List<T> asList() {
        final List<T> results = new ArrayList<T>();
        final MorphiaIterator<T, T> iter = (MorphiaIterator<T, T>) fetch().iterator();
        for (final T ent : iter) {
            results.add(ent);
        }

        if (log.isTraceEnabled()) {
            log.trace(String.format("\nasList: %s \t %d entities, iterator time: driver %n ms, " +
                                            "mapper %n ms \n cache: %s \n for $s \n ",
                                    dbColl.getName(),
                                    results.size(),
                                    iter.getDriverTime(),
                                    iter.getMapperTime(),
                                    cache.stats().toString(),
                                    getQueryObject()));
        }

        return results;
    }


    public List<Key<T>> asKeyList() {
        final List<Key<T>> results = new ArrayList<Key<T>>();
        for (final Key<T> key : fetchKeys()) {
            results.add(key);
        }
        return results;
    }


    public Iterable<T> fetchEmptyEntities() {
        final String[] oldFields = fields;
        final Boolean oldInclude = includeFields;
        fields = new String[]{Mapper.ID_KEY};
        includeFields = true;
        final Iterable<T> res = fetch();
        fields = oldFields;
        includeFields = oldInclude;
        return res;
    }

    /**
     * Converts the textual operator (">", "<=", etc) into a FilterOperator. Forgiving about the syntax; != and <> are
     * NOT_EQUAL, = and == are EQUAL.
     */
    protected FilterOperator translate(String operator) {
        operator = operator.trim();

        if (operator.equals("=") || operator.equals("==")) {
            return FilterOperator.EQUAL;
        }
        else if (operator.equals(">")) {
            return FilterOperator.GREATER_THAN;
        }
        else if (operator.equals(">=")) {
            return FilterOperator.GREATER_THAN_OR_EQUAL;
        }
        else if (operator.equals("<")) {
            return FilterOperator.LESS_THAN;
        }
        else if (operator.equals("<=")) {
            return FilterOperator.LESS_THAN_OR_EQUAL;
        }
        else if (operator.equals("!=") || operator.equals("<>")) {
            return FilterOperator.NOT_EQUAL;
        }
        else if (operator.toLowerCase().equals("in")) {
            return FilterOperator.IN;
        }
        else if (operator.toLowerCase().equals("nin")) {
            return FilterOperator.NOT_IN;
        }
        else if (operator.toLowerCase().equals("all")) {
            return FilterOperator.ALL;
        }
        else if (operator.toLowerCase().equals("exists")) {
            return FilterOperator.EXISTS;
        }
        else if (operator.toLowerCase().equals("elem")) {
            return FilterOperator.ELEMENT_MATCH;
        }
        else if (operator.toLowerCase().equals("size")) {
            return FilterOperator.SIZE;
        }
        else if (operator.toLowerCase().equals("within")) {
            return FilterOperator.WITHIN;
        }
        else if (operator.toLowerCase().equals("near")) {
            return FilterOperator.NEAR;
        }
        else {
            throw new IllegalArgumentException("Unknown operator '" + operator + "'");
        }
    }

    public Query<T> filter(final String condition, final Object value) {
        final String[] parts = condition.trim().split(" ");
        if (parts.length < 1 || parts.length > 6) {
            throw new IllegalArgumentException("'" + condition + "' is not a legal filter condition");
        }

        final String prop = parts[0].trim();
        final FilterOperator op = (parts.length == 2) ? this.translate(parts[1]) : FilterOperator.EQUAL;

        this.add(new FieldCriteria(this, prop, op, value, this.validateName, this.validateType));

        return this;
    }

    public Query<T> where(final CodeWScope js) {
        this.add(new WhereCriteria(js));
        return this;
    }

    public Query<T> where(final String js) {
        this.add(new WhereCriteria(js));
        return this;
    }

    public Query<T> enableValidation() {
        validateName = validateType = true;
        return this;
    }

    public Query<T> disableValidation() {
        validateName = validateType = false;
        return this;
    }

    QueryImpl<T> validateNames() {
        validateName = true;
        return this;
    }

    QueryImpl<T> disableTypeValidation() {
        validateType = false;
        return this;
    }

    public T get() {
        final int oldLimit = limit;
        limit = 1;
        final Iterator<T> it = fetch().iterator();
        limit = oldLimit;
        return (it.hasNext()) ? it.next() : null;
    }


    public Key<T> getKey() {
        final int oldLimit = limit;
        limit = 1;
        final Iterator<Key<T>> it = fetchKeys().iterator();
        limit = oldLimit;
        return (it.hasNext()) ? it.next() : null;
    }


    public Query<T> limit(final int value) {
        this.limit = value;
        return this;
    }

    public Query<T> batchSize(final int value) {
        this.batchSize = value;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Query<T> skip(final int value) {
        this.offset = value;
        return this;
    }

    public Query<T> offset(final int value) {
        this.offset = value;
        return this;
    }


    public Query<T> order(final String condition) {
        if (snapshotted) {
            throw new QueryException("order cannot be used on a snapshotted query.");
        }

        //reset order
        if (condition == null || condition.trim() == "") {
            sort = null;
        }

        sort = parseFieldsString(condition, clazz, this.ds.getMapper(), this.validateName);

        return this;
    }

    /**
     * parses the string and validates each part
     */
    @SuppressWarnings("rawtypes")
    public static BasicDBObject parseFieldsString(final String str, final Class clazz, final Mapper mapr,
                                                  final boolean validate) {
        BasicDBObjectBuilder ret = BasicDBObjectBuilder.start();
        final String[] parts = str.split(",");
        for (String s : parts) {
            s = s.trim();
            int dir = 1;

            if (s.startsWith("-")) {
                dir = -1;
                s = s.substring(1).trim();
            }

            if (validate) {
                final StringBuffer sb = new StringBuffer(s);
                Mapper.validate(clazz, mapr, sb, FilterOperator.IN, "", true, false);
                s = sb.toString();
            }
            ret = ret.add(s, dir);
        }
        return (BasicDBObject) ret.get();
    }

    public Iterator<T> iterator() {
        return fetch().iterator();
    }

    public Iterator<T> tail() {
        return tail(true);
    }

    public Iterator<T> tail(final boolean awaitData) {
        //Create a new query for this, so the current one is not affected.
        final QueryImpl<T> tailQ = clone();
        tailQ.tail = true;
        tailQ.tail_await_data = awaitData;
        return tailQ.fetch().iterator();
    }

    public Class<T> getEntityClass() {
        return this.clazz;
    }

    public String toString() {
        return this.getQueryObject().toString();
    }

    public FieldEnd<? extends Query<T>> field(final String name) {
        return this.field(name, this.validateName);
    }

    private FieldEnd<? extends Query<T>> field(final String field, final boolean validate) {
        return new FieldEndImpl<QueryImpl<T>>(this, field, this, validate);
    }

    public FieldEnd<? extends CriteriaContainerImpl> criteria(final String field) {
        return this.criteria(field, this.validateName);
    }

    private FieldEnd<? extends CriteriaContainerImpl> criteria(final String field, final boolean validate) {
        final CriteriaContainerImpl container = new CriteriaContainerImpl(this, CriteriaJoin.AND);
        this.add(container);

        return new FieldEndImpl<CriteriaContainerImpl>(this, field, container, validate);
    }

    //TODO: test this.
    public Query<T> hintIndex(final String idxName) {
        indexHint = idxName;
        return this;
    }

    public Query<T> retrievedFields(final boolean include, final String... fields) {
        if (includeFields != null && include != includeFields) {
            throw new IllegalStateException("You cannot mix include and excluded fields together!");
        }
        this.includeFields = include;
        this.fields = fields;
        return this;
    }

    public Query<T> retrieveKnownFields() {
        final MappedClass mc = this.ds.getMapper().getMappedClass(clazz);
        final ArrayList<String> fields = new ArrayList<String>(mc.getPersistenceFields().size() + 1);
        for (final MappedField mf : mc.getPersistenceFields()) {
            fields.add(mf.getNameToStore());
        }
        retrievedFields(true, (String[]) fields.toArray());
        return this;
    }

    /**
     * Enabled snapshotted mode where duplicate results (which may be updated during the lifetime of the cursor) will
     * not
     * be returned. Not compatible with order/sort and hint.
     */
    public Query<T> enableSnapshotMode() {
        snapshotted = true;
        return this;
    }

    /**
     * Disable snapshotted mode (default mode). This will be faster but changes made during the cursor may cause
     * duplicates. *
     */
    public Query<T> disableSnapshotMode() {
        snapshotted = false;
        return this;
    }

    public Query<T> useReadPreference(final ReadPreference readPref) {
        this.readPref = readPref;
        return this;
    }

    public Query<T> queryNonPrimary() {
        readPref = ReadPreference.secondaryPreferred();
        return this;
    }

    public Query<T> queryPrimaryOnly() {
        readPref = ReadPreference.primary();
        return this;
    }

    /**
     * Disables cursor timeout on server.
     */
    public Query<T> disableCursorTimeout() {
        noTimeout = true;
        return this;
    }

    /**
     * Enables cursor timeout on server.
     */
    public Query<T> enableCursorTimeout() {
        noTimeout = false;
        return this;
    }

    @Override
    public String getFieldName() {
        return null;
    }


}
