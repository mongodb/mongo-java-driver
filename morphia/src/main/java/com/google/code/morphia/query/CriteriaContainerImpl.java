package com.google.code.morphia.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class CriteriaContainerImpl extends AbstractCriteria implements Criteria, CriteriaContainer {
	protected CriteriaJoin joinMethod;
	protected List<Criteria> children;
	
	protected QueryImpl<?> query;
	
	protected CriteriaContainerImpl(CriteriaJoin joinMethod) {
		this.joinMethod = joinMethod;
		this.children = new ArrayList<Criteria>();
	}
	
	protected CriteriaContainerImpl(QueryImpl<?> query, CriteriaJoin joinMethod) {
		this(joinMethod);
		this.query = query;
	}
	
	public void add(Criteria... criteria) {
		for (Criteria c: criteria) {
			c.attach(this);
			this.children.add(c);
		}
	}
	
	public void remove(Criteria criteria) {
		this.children.remove(criteria);
	}
	
	public void addTo(DBObject obj) {
		if (this.joinMethod == CriteriaJoin.AND) {
			Set<String> fields = new HashSet<String>();
			int nonNullFieldNames = 0;
			for (Criteria child: this.children) {
				if (null != child.getFieldName()) {
					fields.add(child.getFieldName());
					nonNullFieldNames++;
				}
			}
			if(fields.size() < nonNullFieldNames) {
				//use $and
				BasicDBList and = new BasicDBList();

				for (Criteria child: this.children) {
					BasicDBObject container = new BasicDBObject();
					child.addTo(container);
					and.add(container);
				}
				
				obj.put("$and", and);
			} else {
				//no dup field names, don't use $and
				for (Criteria child: this.children) {
					child.addTo(obj);
				}
			}
		} else if (this.joinMethod == CriteriaJoin.OR) {
			BasicDBList or = new BasicDBList();

			for (Criteria child: this.children) {
				BasicDBObject container = new BasicDBObject();
				child.addTo(container);
				or.add(container);
			}
			
			obj.put("$or", or);
		}
	}
	
	public CriteriaContainer and(Criteria... criteria) {
		return collect(CriteriaJoin.AND, criteria);
	}
	
	public CriteriaContainer or(Criteria... criteria) {
		return collect(CriteriaJoin.OR, criteria);
	}
	
	private CriteriaContainer collect(CriteriaJoin cj, Criteria... criteria) {
		CriteriaContainerImpl parent = new CriteriaContainerImpl(this.query, cj);
		
		for (Criteria c: criteria)
			parent.add(c);
		
		add(parent);
		
		return parent;		
	}
	
	public FieldEnd<? extends CriteriaContainer> criteria(String name) {
		return this.criteria(name, this.query.isValidatingNames());
	}
	
	private FieldEnd<? extends CriteriaContainer> criteria(String field, boolean validateName) {
		return new FieldEndImpl<CriteriaContainerImpl>(this.query, field, this, validateName);
	}

	public String getFieldName() {
		return joinMethod.toString();
	}
}
