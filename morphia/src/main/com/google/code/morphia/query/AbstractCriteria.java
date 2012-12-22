package com.google.code.morphia.query;

public abstract class AbstractCriteria implements Criteria {
	protected CriteriaContainerImpl attachedTo = null;
	
	public void attach(CriteriaContainerImpl container) {
		if (this.attachedTo != null) {
			this.attachedTo.remove(this);
		}
		
		this.attachedTo = container;
	}
}
