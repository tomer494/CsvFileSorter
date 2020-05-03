package com.ob1tech.CsvFileSorter.algorithms.sort;

import com.ob1tech.CsvFileSorter.dateModel.IndexNode;

public abstract class IndexHeap<T extends Comparable<T>> extends MinIndexedBinaryHeap<T> {

	public IndexHeap(int maxSize) {
		super(maxSize);
		// TODO Auto-generated constructor stub
	}

	protected abstract void doInnerSwap(IndexNode<T> node1, IndexNode<T> node2);

}
