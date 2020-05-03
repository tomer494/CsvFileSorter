package com.ob1tech.CsvFileSorter.dateModel;

/**
 * It contains the sorting logic witch means that a batch is
 * highr only if alll its inner components are higher then the other, same as lower
 * otherwise will calculated as compared {@link #compareTo(SortKey)}.
 * @author Madmon Tomer
 *
 * @param <T>
 */
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


	/**
	 * A sort key is only higher if bouth min and max are highr.
	 * A sort key is only smaller if bouth min and max are smaller.
	 * Otherwise it is considered equal and the program should deel with that.
	 */
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
