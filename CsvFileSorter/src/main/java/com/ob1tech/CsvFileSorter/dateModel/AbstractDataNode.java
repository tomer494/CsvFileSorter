/**
 * 
 */
package com.ob1tech.CsvFileSorter.dateModel;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class represents a generic record Node.
 * It stores an id
 * and a comparable {@link #key} used for sorting the records.
 * @author Madmon Tomer
 * @param <T> comparable record key value
 * @see Comparable
 */
public abstract class AbstractDataNode<T> 
	implements Comparable<SortKey<T>>, Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8864441135292110978L;

	@JsonProperty("id")
	private Long id;
	
	/**
	 * This is the valueing key of the referencing value.
	 * Key is a comparable key of a <T> type
	 * @see Comparable
	 */
	@JsonProperty("key")
	private SortKey<T> key;

	public AbstractDataNode() {
		super();
	}
	
	public AbstractDataNode(Long id) {
		super();
		this.id = id;
	}
	
	public AbstractDataNode(SortKey<T> key, Long id) {
		super();
		this.key = key;
		this.id = id;
	}
	
	public SortKey<T> getKey() {
		return key;
	}

	public void setKey(SortKey<T> key) {
		this.key = key;
	}

	@Override
	public int compareTo(SortKey<T> o) {
		return this.getKey().compareTo(o);
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

}
