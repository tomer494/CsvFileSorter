package com.ob1tech.CsvFileSorter.dateModel;

import java.util.LinkedList;
import java.util.List;

/**
 * This class represents an Record Batch Node data model.
 * 
 * @author Madmon Tomer
 *
 * @param <T>
 */
public class RecordBatchNode<T extends Comparable<T>>
	extends AbstractDataNode<T> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1557549805327292117L;
	
	private List<RecordIndex<T>> records;
	
	public RecordBatchNode(long id) {
		super(id);
		this.setRecords(records);
		records = new LinkedList<RecordIndex<T>>();
	}

	@Override
	public int compareTo(SortKey<T> o) {
		return this.getKey().compareTo(o);
	}
	
	@Override
	public String toString() {
		return getKey().toString();
	}

	/**
	 * Appending a new RecordIndex in and calculating the new min/max value sortKey
	 * @param node
	 */
	@SuppressWarnings("unchecked")
	public void insert(RecordIndex<T> node) {
		T maxValue = null;
		T minValue = null;
		SortKey<T> key = getKey();
		if(key==null) {
			key = new SortKey<T>(minValue, maxValue);
			setKey(key);
		}else {
			maxValue = (T) key.getMaxValue();
			minValue = (T) key.getMinValue();
		}
		if(maxValue==null) {
			minValue = maxValue = (T) node.getKey();
		}else {
			if(maxValue.compareTo((T) node.getKey())<0) {		
				maxValue = (T) node.getKey();
			}
			if(minValue.compareTo((T) node.getKey())>0){
				minValue = (T) node.getKey();
			}
		}
		key.setMaxValue(maxValue);
		key.setMinValue(minValue);

		getRecords().add(node);
		
	}
	
	/**
	 * clear batch data and reset min/max values
	 */
	public void reset() {
		getRecords().clear();
		SortKey<T> key = getKey();
		key.setMaxValue(null);
		key.setMinValue(null);
	}

	public List<RecordIndex<T>> getRecords() {
		return records;
	}

	public void setRecords(List<RecordIndex<T>> records) {
		this.records = records;
	}
	

	
}
