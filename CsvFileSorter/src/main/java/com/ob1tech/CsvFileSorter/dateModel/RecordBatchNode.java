package com.ob1tech.CsvFileSorter.dateModel;

import java.util.LinkedList;
import java.util.List;

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

	//List<Record<T>> records;

	@Override
	public int compareTo(SortKey<T> o) {
		return this.getKey().compareTo(o);
	}
	
	@Override
	public String toString() {
		return getKey().toString();
	}

	@SuppressWarnings("unchecked")
	public void insert(RecordIndex<T> node) {
		//notFullOrThrowError();
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
	
	/*
	@JsonProperty("heapedData")
	private List<AbstractIndexNode<R,T>> heapedData;
	
	@JsonProperty("maxLength")
	private int maxLength;
	
	@JsonProperty("maxValue")
	private T maxValue;



	public RecordsNode(int maxLength) {
		super();
		this.maxLength = maxLength;
		heapedData = new LinkedList<AbstractIndexNode<R,T>>();
	}

	
	@Override
	public int compareTo(IndexKey<T> o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	public int getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}
	
	public List<AbstractIndexNode<R,T>> getHeapedData() {
		return heapedData;
	}

	public void setHeapedData(List<AbstractIndexNode<R,T>> HeapedData) {
		this.heapedData = HeapedData;
	}


	
	
	private void notFullOrThrowError() {
		if(isFull()) {
			throw new IndexOutOfBoundsException("List is full");
		}
		
	}

	private boolean isFull() {
		return heapedData.size()==maxLength;
	}

	public void reset() {
		heapedData.clear();
		maxValue = null;
		
	}
	
	public void insert(AbstractIndexNode<R,T> node) {
		insert(heapedData.size(), node);
	}
	
	public T getMaxValue() {
		return maxValue;
	}



	public void setMaxValue(T maxValue) {
		this.maxValue = maxValue;
	}
*/


	
	
}
