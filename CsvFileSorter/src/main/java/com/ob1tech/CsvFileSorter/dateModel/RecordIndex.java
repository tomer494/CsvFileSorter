package com.ob1tech.CsvFileSorter.dateModel;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Minimal {@link AbstractDataNode} implementation
 * @see AbstractDataNode
 * @author Madmon Tomer
 *
 * @param <R> record
 * @param String record key value
 *
 */
public class RecordIndex<T extends Comparable<T>> implements Comparable<RecordIndex<T>>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2695084355198124388L;

	@JsonProperty("recordLine")
	long recordLine;

	@JsonProperty("record")
	String record;

	@JsonProperty("key")
	T key;
	
	public RecordIndex(long recordLine, T key) {
		this.key = key;
		this.recordLine = recordLine;
	}

	public RecordIndex(long recordLine, T key, String record) {
		this.key = key;
		this.recordLine = recordLine;
		this.record = record;
	}

	public RecordIndex(Object obj) {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(RecordIndex<T> o) {
		return this.key.compareTo(o.key);
	}
	
	@Override
	public String toString() {
		return "" + key;
	}

	public T getKey() {
		return key;
	}

	public void setKey(T key) {
		this.key = key;
	}

	public long getRecordLine() {
		return recordLine;
	}

	public void setRecordLine(long recordLine) {
		this.recordLine = recordLine;
	}

	public String getRecord() {
		return record;
	}

	public void setRecord(String record) {
		this.record = record;
	}
	

}
