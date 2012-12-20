package com.google.code.morphia.mapping.cache;

// note that it is not threadsafe, currently it does not need to be.
public class EntityCacheStatistics {
	int entities;
	int hits;
	int misses;
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName() + ": " + entities + " entities, " + hits + " hits, " + misses + " misses.";
	}
	
	public void reset() {
		entities = 0;
		hits = 0;
		misses = 0;
	}
	
	public EntityCacheStatistics copy() {
		EntityCacheStatistics copy = new EntityCacheStatistics();
		copy.entities = entities;
		copy.hits = hits;
		copy.misses = misses;
		return copy;
	}
}
