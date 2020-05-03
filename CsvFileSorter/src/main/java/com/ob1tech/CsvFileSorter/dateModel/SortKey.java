package com.ob1tech.CsvFileSorter.dateModel;

public class SortKey<T> implements Comparable<SortKey<T>> {
	
	private Comparable<T> maxValue;
	private Comparable<T> minValue;

	public SortKey(Comparable<T> minValue, Comparable<T> maxValue) {
		super();
		this.maxValue = maxValue;
		this.minValue = minValue;
	}


	@Override
	public String toString() {
		return "(" + minValue + ", " + maxValue + ")";
	}


	@SuppressWarnings("unchecked")
	@Override
	public int compareTo(SortKey<T> o) {
		if(minValue.compareTo((T) ((SortKey<T>)o).getMaxValue())>0) {
			return 1;			
		} else if(maxValue.compareTo((T) ((SortKey<T>)o).getMinValue())<0) {
			return -1;			
		}
		
		return 0;
	}
	

	public Comparable<T> getMaxValue() {
		return maxValue;
	}

	public void setMaxValue(Comparable<T> maxValue) {
		this.maxValue = maxValue;
	}

	public Comparable<T> getMinValue() {
		return minValue;
	}

	public void setMinValue(Comparable<T> minValue) {
		this.minValue = minValue;
	}

}
