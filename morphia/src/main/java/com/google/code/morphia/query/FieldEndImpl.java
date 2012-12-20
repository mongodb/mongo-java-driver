package com.google.code.morphia.query;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.MorphiaLoggerFactory;
import com.google.code.morphia.utils.Assert;

public class FieldEndImpl<T extends CriteriaContainerImpl> implements FieldEnd<T> {
	private static final Logr log = MorphiaLoggerFactory.get(FieldEndImpl.class);

	private QueryImpl<?> query;
	private String field;
	private T target;
	private boolean not;
	private boolean validateName;
	
	private FieldEndImpl(QueryImpl<?> query, String field, T target, boolean validateName, boolean not) {
		this.query = query;
		this.field = field;
		this.target = target;
		this.validateName = validateName;
		this.not = not;
	}
	
	public FieldEndImpl(QueryImpl<?> query, String field, T target, boolean validateName) {
		
		this(query, field, target,validateName, false);
	}
	
	/** Add a criteria */
	private T addCrit(FilterOperator op, Object val) {
		target.add(new FieldCriteria(query, field, op, val, validateName, query.isValidatingTypes(), not));
		return target;
	}

	private T addGeoCrit(FilterOperator op, Object val, Map<String, Object> opts) {
		if (not) 
			throw new QueryException("Geospatial queries cannot be negated with 'not'.");
		
		target.add(new GeoFieldCriteria(query, field, op, val, validateName, false, opts));
		return target;
	}
	
	public FieldEnd<T> not() {
		not = !not;
		return this;
	}
	public T startsWith(String prefix) {
		Assert.parametersNotNull("val", prefix);
		return addCrit(FilterOperator.EQUAL, Pattern.compile("^" + prefix));
	}

	public T startsWithIgnoreCase(String prefix) {
		Assert.parametersNotNull("val", prefix);
		return addCrit(FilterOperator.EQUAL, Pattern.compile("^" + prefix, Pattern.CASE_INSENSITIVE));
	}

	public T endsWith(String suffix) {
		Assert.parametersNotNull("val", suffix);
		return addCrit(FilterOperator.EQUAL, Pattern.compile(suffix + "$"));
	}

	public T endsWithIgnoreCase(String suffix) {
		Assert.parametersNotNull("val", suffix);
		return addCrit(FilterOperator.EQUAL, Pattern.compile(suffix + "$", Pattern.CASE_INSENSITIVE));
	}
	
	public T contains(String string) {
		Assert.parametersNotNull("val", string);
		return addCrit(FilterOperator.EQUAL, Pattern.compile(string));
	}

	public T containsIgnoreCase(String string) {
		Assert.parametersNotNull("val", string);
		return addCrit(FilterOperator.EQUAL, Pattern.compile(string, Pattern.CASE_INSENSITIVE));
	}

	public T exists() {
		return addCrit(FilterOperator.EXISTS, true);
	}

	public T doesNotExist() {
		return addCrit(FilterOperator.EXISTS, false);
	}

	public T equal(Object val) {
		return addCrit(FilterOperator.EQUAL, val);
	}
	
	public T greaterThan(Object val) {
		Assert.parametersNotNull("val",val);
		return addCrit(FilterOperator.GREATER_THAN, val);
	}

	public T greaterThanOrEq(Object val) {
		Assert.parametersNotNull("val",val);
		return addCrit(FilterOperator.GREATER_THAN_OR_EQUAL, val);
	}

	public T hasThisOne(Object val) {
		return addCrit(FilterOperator.EQUAL, val);
	}

	public T hasAllOf(Iterable<?> vals) {
		Assert.parametersNotNull("vals",vals);
		Assert.parameterNotEmpty(vals,"vals");
		return addCrit(FilterOperator.ALL, vals);
	}

	public T hasAnyOf(Iterable<?> vals) {
		Assert.parametersNotNull("vals",vals);
//		Assert.parameterNotEmpty(vals,"vals"); //it is valid but will never return any results.
		if (log.isWarningEnabled()) {
			if (!vals.iterator().hasNext())
				log.warning("Specified an empty list/collection with the '" + field + "' criteria");
		}
		return addCrit(FilterOperator.IN, vals);
	}
	
	public T in(Iterable<?> vals) {
		return this.hasAnyOf(vals);
	}

	public T hasThisElement(Object val) {
		Assert.parametersNotNull("val",val);
		return addCrit(FilterOperator.ELEMENT_MATCH, val);
	}

	public T hasNoneOf(Iterable<?> vals) {
		Assert.parametersNotNull("vals",vals);
		Assert.parameterNotEmpty(vals,"vals");
		return addCrit(FilterOperator.NOT_IN, vals);
	}
	
	public T notIn(Iterable<?> vals) {
		return this.hasNoneOf(vals);
	}

	public T lessThan(Object val) {
		Assert.parametersNotNull("val",val);
		return addCrit(FilterOperator.LESS_THAN, val);
	}

	public T lessThanOrEq(Object val) {
		Assert.parametersNotNull("val",val);
		return addCrit(FilterOperator.LESS_THAN_OR_EQUAL, val);
	}

	public T notEqual(Object val) {
		return addCrit(FilterOperator.NOT_EQUAL, val);
	}

	public T sizeEq(int val) {
		return addCrit(FilterOperator.SIZE, val);
	}
	
	public T near(double x, double y) {
		return near(x, y, false);
	}

	public T near(double x, double y, double radius) {
		return near(x, y, radius,  false);
	}
	public T near(double x, double y, double radius, boolean spherical) {
		return addGeoCrit(spherical ? FilterOperator.NEAR_SPHERE : FilterOperator.NEAR, new double[] {x,y},  opts("$maxDistance", radius));
	}	
	public T near(double x, double y, boolean spherical) {
		return addGeoCrit(spherical ? FilterOperator.NEAR_SPHERE : FilterOperator.NEAR, new double[] {x,y}, null);
	}
	
	public T within(double x, double y, double radius) {
		return within(x, y, radius, false);
	}
	
	public T within(double x, double y, double radius, boolean spherical) {
		return addGeoCrit(spherical ? FilterOperator.WITHIN_CIRCLE_SPHERE : FilterOperator.WITHIN_CIRCLE, new Object[] { new double[] { x, y }, radius }, null);
	}
	
	public T within(double x1, double y1, double x2, double y2) {
		return addGeoCrit(FilterOperator.WITHIN_BOX, new double[][] {new double[] {x1 , y1}, new double[] {x2,y2}}, null);
	}
	
	private Map<String, Object> opts(String s, Object v) {
		Map<String, Object> opts = new HashMap<String, Object>();
		opts.put(s, v);
		return opts;
	}
	
	private Map<String, Object> opts(String s1, Object v1, String s2, Object v2) {
		Map<String, Object> opts = new HashMap<String, Object>();
		opts.put(s1, v1);
		opts.put(s2, v2);
		return opts;
	}
	
}
