package com.ob1tech.CsvFileSorter.algorithms.sort;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ob1tech.CsvFileSorter.dateModel.IndexNode;


/**
 * <pre>
 * MinIndexedBinaryHeap is a class means to use a priority index
 * queue sorting on applied comparable values.
 * Max complexity level is O(n)
 * {@link #valueOf(int)}		| O(1)
 * {@link #pollMinKeyIndex()}	| O(log(n))
 * {@link #insert(Comparable)}	| O(log(n))
 * {@link #contains(int)}		| O(1)
 * {@link #delete(int)}		| O(log(n))
 * {@link #peekMinKeyIndex()}	| O(1)
 * {@link #peekMinValue()}		| O(1)
 * {@link #pollMinValue()}		| O(log(n))
 * {@link #update(int, Comparable)}| O(log(n))
 * 
 * Example: 
 * VALUE = Comparable value
 * KEY = entered order
 * VALUE	| KEY
 * 2	| 0
 * 4	| 1
 * 5	| 2
 * 1	| 3
 * values[]:[2,4,5,1]
 * positionMap[]:[1,3,2,0] Example: the 2nd value (key=1) is at node 3
 * inverseMap[]:[3,0,2,1] Example: at the 2nd node (node=1) the key is 0
 * 
 * Tree:
 *		0(k=3,V=1)
 * 		/	\
 * 	1(k=0,V=2)	2(k=2,V=5)
 * 	/
 * 3(k=1,V=4)
 * </pre>
 * 
 * @author Madmon Tomer
 * 
 *         <p>
 *         Based on code provided by: William Fiset,
 *         william.alexandre.fiset@gmail.com {@link <a href=
 *         "https://github.com/williamfiset/data-structures">williamfiset
 *         data-structures at github<a>}<p>
 * 
 * 
 * @param <T> comparable value
 * @param <IndexNode<T>>
 * @see Comparable
 */
public abstract class MinIndexedBinaryTree<T> implements Iterable<T>{

	/**
	 * @Current number of elements in the heap.
	 */
	private AtomicLong treeSize;
	
	private IndexNode<T> root;

	Logger logger = LogManager.getLogger(MinIndexedBinaryTree.class);
	
	

	/**
	 * Contractor: Initializes a binary heap with a maximum capacity of maxSize.
	 * occur if maxSize &lt;= 0
	 * 
	 * @param maxSize max heap size allowed for processing
	 * @exception IllegalArgumentException
	 * @see IllegalArgumentException
	 */
	public MinIndexedBinaryTree() {

		treeSize = new AtomicLong();
		root = null;
	}

	/**
	 * Get current heap size
	 * 
	 * @return long Current heap size
	 */
	public long size() {
		return treeSize.get();
	}

	/**
	 * insert a new value to a non full heap. Full heap will throw an error
	 * Balance the heap to keep it as a min heap. 
	 * The value will be added to the last appended to the heap and
	 * then will be bubble(swim) up as needed.
	 * @param value comparable value
	 * @see #insert(int, Comparable)
	 */
	public boolean add(IndexNode<T> value) {
		
		if(logger.isDebugEnabled()) {
			printTree("Add: "+value);
		}
		
		long nodeIndex = treeSize.getAndIncrement();
		value.setId(nodeIndex);
		save(nodeIndex, value);
		
		if(root==null) {
			root = value;
		}else {
			IndexNode<T> pointer = root;
			//root = 
			add(pointer, value, nodeIndex, null);
		}
		if(logger.isDebugEnabled()) {
			printTree("Post: ");
		}
		
		return false;
	}

	public IndexNode<T> getRoot() {
		return root;
	}

	public void printTree(String title) {
		logger.trace(title);
		
		@SuppressWarnings("rawtypes")
		Iterator it = iterator();
		while(it.hasNext()) {
			Object next = it.next();
			logger.trace(next);
		}
		logger.trace("--------------");
	}
	
	protected IndexNode<T> goLeft(IndexNode<T> pointer, IndexNode<T> value, Long nodeIndex, IndexNode<T> parentNode) {
		Long leafNodeIndex = pointer.getLeftNode();
		if(leafNodeIndex!=null) {
			pointer = getValueOf(leafNodeIndex);
		}else {
			if(parentNode==null){
				pointer.setLeftNode(nodeIndex);			
				save(pointer.getId(), pointer);
			}
			return null;
		}
		return pointer;
	}

	protected IndexNode<T> goRight(IndexNode<T> pointer, IndexNode<T> value, Long nodeIndex, IndexNode<T> parentNode) {
		Long leafNodeIndex = pointer.getRightNode();
		if(leafNodeIndex!=null) {
			pointer = getValueOf(leafNodeIndex);
		}else {
			if(parentNode==null){
				pointer.setRightNode(nodeIndex);			
				save(pointer.getId(), pointer);
			}
			return null;
		}
		return pointer;
	}

	protected void add(IndexNode<T> pointer, IndexNode<T> value, long nodeIndex, IndexNode<T> parentNode) {
		
		while(pointer!=null) {
			logger.debug("pointer"+pointer.getKey()+",node"+value.getKey());
			if(value.getKey().compareTo(pointer.getKey())<0) {
				pointer = goLeft(pointer, value, nodeIndex, parentNode);
			}else if(value.getKey().compareTo(pointer.getKey())>0) {
				pointer = goRight(pointer, value, nodeIndex, parentNode);
			}else{
				pointer = handleMixedValues(pointer, value, nodeIndex, parentNode);		
				
			}
		}

	}

	protected IndexNode<T> handleMixedValues(IndexNode<T> pointer, IndexNode<T> value, long nodeIndex, IndexNode<T> parentNode) {
		doInnerSwap(value, pointer);
		//Split lows to right, heighs to left
		//value is left with low valued, so go right
		if(pointer.getLeftNode()!=null) {
			sendPointerToLeft(pointer, value, parentNode);
		}
		return goRight(pointer, value, nodeIndex, parentNode);
	}

	public void sendPointerToLeft(IndexNode<T> pointer, IndexNode<T> value, IndexNode<T> parentNode) {
		
		IndexNode<T> leftNode = pointer;
		IndexNode<T> leftPointer = getValueOf(leftNode.getLeftNode());
		add(leftPointer, leftNode, leftNode.getId(), value);
			
	}

	protected abstract void save(long nodeIndex, IndexNode<T> value);

	protected abstract IndexNode<T> getValueOf(long nodeIndex);
	
	protected abstract void doInnerSwap(IndexNode<T> lowerLevelNode, IndexNode<T> higherLevelNode);

	//protected abstract void swap(long nodeIndex, long parentIndex);

	/**
	 * Heaped value polling iterator
	 * @author Madmon Tomer
	 *
	 */
	public class MinIndexedBinaryTreeIterator implements Iterator<IndexNode<T>> {
		
		private long read = 0;
		private long startAtHeapSize;
		private Stack<Long> nextIndexStack;
		private IndexNode<T> pointer;

		public MinIndexedBinaryTreeIterator(long startAtHeapSize) {
			read = 0;
			this.startAtHeapSize = startAtHeapSize;
			nextIndexStack = new Stack<Long>();
			nextIndexStack.add(0l);
			pointer = root;
		}
		
		private void noConcurencyUpdateOrThrow() {
			if(startAtHeapSize != treeSize.get()) {
				throw new ConcurrentModificationException();
			}
		}

		public boolean hasNext() {
			noConcurencyUpdateOrThrow();
			return root!=null && !nextIndexStack.isEmpty();
		}

		public IndexNode<T> next() {
			noConcurencyUpdateOrThrow();
			
			while(pointer!=null && pointer.getLeftNode()!=null) {
				nextIndexStack.push(pointer.getLeftNode());
				pointer = getValueOf(pointer.getLeftNode());
			}
			
			IndexNode<T> node = getValueOf(nextIndexStack.pop());
			
			if(node.getRightNode()!=null) {
				nextIndexStack.push(node.getRightNode());
				pointer = getValueOf(node.getRightNode());
			}
			
			return node;
		}

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Iterator iterator() {
		return new MinIndexedBinaryTreeIterator(size());
	}
	

}