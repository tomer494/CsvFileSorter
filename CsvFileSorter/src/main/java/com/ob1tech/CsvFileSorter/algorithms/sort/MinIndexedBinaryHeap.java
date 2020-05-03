package com.ob1tech.CsvFileSorter.algorithms.sort;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


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
 * @see Comparable
 */
public class MinIndexedBinaryHeap<T extends Comparable<T>> implements Iterable<T>{

	/**
	 * @Current number of elements in the heap.
	 */
	private int heapSize;

	/**
	 * Maximum number of elements in the heap.
	 */
	private final int maxHeapSize;

	/**
	 * Lookup arrays to track the child/parent indexes of each node. Save time on
	 * simple calculations. For every node in position i store the position of it's
	 * parent and the minimum valued child node. Note: the next valued child is
	 * always +1 index
	 * 
	 * parent of node [i] is at position x = ((i - 1) / 2) in the array of nodes min
	 * child of node [i] is at position x = (i * 2 + 1) and the next is at x+1;
	 * Example: the children of node indexed 1 are at indexes [3,4] and the parent
	 * index is at [0]
	 */
	private final int[] child, parent;

	/**
	 * The Position Map (positionMap) keeps the position of the node in the heap
	 * tree. It maps Key Indexes (key) to where the position of that key is
	 * represented in the priority queue in the domain [0, heapSize).
	 * 
	 * See class notation values example. Example: the 2nd value (key=1) is at node
	 * 3
	 */
	public final int[] positionMap;

	/**
	 * The Inverse Map (inverseMap) keeps the positions of the keys in the node
	 * array. It stores the indexes of the keys in the range [0, heapSize) which
	 * make up the priority queue. It should be noted that 'inverseMap' and
	 * 'positionMap' are inverses of each other, so: positionMap[inverseMap[i]] =
	 * inverseMap[positionMap[i]] = i
	 * 
	 * See class notation values example. Example: at the 2nd node (node=1) the key
	 * is 0
	 */
	public final int[] inverseMap;

	/**
	 * The values associated with the keys. It is very important to note that this
	 * array is indexed by the key indexes (aka 'key'). The key is the same as the
	 * insertion order of values
	 */
	public final Object[] values;


	public int getMaxHeapSize() {
		return maxHeapSize;
	}
	

	/**
	 * Contractor: Initializes a binary heap with a maximum capacity of maxSize.
	 * Expected maxSize &gt; 2 otherwise this sorting tool is useless. Error will
	 * occur if maxSize &lt;= 0
	 * 
	 * @param maxSize max heap size allowed for processing
	 * @exception IllegalArgumentException
	 * @see IllegalArgumentException
	 */
	public MinIndexedBinaryHeap(int maxSize) {
		if (maxSize < 2)
			throw new IllegalArgumentException(maxSize+" < 2. too low");

		maxHeapSize = maxSize;

		// Initialization
		inverseMap = new int[maxHeapSize];
		positionMap = new int[maxHeapSize];
		child = new int[maxHeapSize];
		parent = new int[maxHeapSize];
		values = new Object[maxHeapSize];

		for (int i = 0; i < maxHeapSize; i++) {
			parent[i] = parentIndex(i);
			child[i] = minChildIndex(i);
			positionMap[i] = inverseMap[i] = -1;
		}
	}
	
	public void clear() {
		for (int i = 0; i < maxHeapSize; i++) {
			positionMap[i] = inverseMap[i] = -1;
			values[i] = null;
		}
		heapSize = 0;
	}

	/**
	 * Min(left leaf) child index formula
	 * @param nodeIndex parent node index
	 * @return min child index
	 */
	public static int minChildIndex(int nodeIndex) {
		return nodeIndex * 2 + 1;
	}

	/**
	 * Parent node index formula
	 * @param nodeIndex child node index
	 * @return parent index
	 */
	public static int parentIndex(int nodeIndex) {
		return (nodeIndex - 1) / 2;
	}

	/**
	 * Get current heap size
	 * 
	 * @return int Current heap size
	 */
	public int size() {
		return heapSize;
	}

	/**
	 * Check if current heap is empty
	 * 
	 * @return boolean true if heap has no elements
	 */
	public boolean isEmpty() {
		return heapSize == 0;
	}

	/**
	 * Check if inserted key is in the bounds of the heap and points to occupied
	 * position. Out of bound key will result in an error.
	 * 
	 * @param key value position
	 * @return boolean true if heap has no elements
	 * @see #keyInBoundsOrThrow(int)
	 */
	public boolean contains(int key) {
		keyInBoundsOrThrow(key);
		return positionMap[key] != -1;
	}

	/**
	 * Get the minimal value position in heap aka 'key'.
	 * 
	 * @return minimal value key
	 * @see #isNotEmptyOrThrow()
	 */
	public int peekMinKeyIndex() {
		isNotEmptyOrThrow();
		return inverseMap[0];
	}

	/**
	 * Remove the minimal value position in the heap.
	 * 
	 * @return minimal value key
	 * @see #peekMinKeyIndex()
	 * @see #delete(int)
	 */
	public int pollMinKeyIndex() {
		int minKey = peekMinKeyIndex();
		delete(minKey);
		return minKey;
	}

	/**
	 * Get the minimal value in heap. Empty heap throws an error.
	 * 
	 * @return minimal value
	 * @see #peekMinKeyIndex()
	 */
	@SuppressWarnings("unchecked")
	public T peekMinValue() {
		int key = peekMinKeyIndex();
		return (T) values[key];
	}

	/**
	 * Extract the minimal value in heap.
	 * 
	 * @return minimal value
	 * @see #peekMinValue()
	 * @see #peekMinKeyIndex()
	 * @see #delete(int)
	 */
	public T pollMinValue() {
		T minValue = peekMinValue();
		delete(peekMinKeyIndex());
		return minValue;
	}

	/**
	 * insert a new value to a non full heap. Full heap will throw an error
	 * 
	 * @param value comparable value
	 * @see #insert(int, Comparable)
	 */
	public void insert(T value) {
		// Next available position
		int key = heapSize;
		insert(key, value);
	}

	/**
	 * insert value to a valid(Not occupied) key position and balance the heap to
	 * keep it as a min heap. The key will be added to the last node on the heap and
	 * then will be bubble(swim) up as needed.
	 * <p>
	 * Errors will occur when:
	 * <ul>
	 * <li>Position is occupied
	 * <li>Value is null
	 * </ul>
	 * 
	 * @param value
	 * @see #contains(int)
	 * @see #valueNotNullOrThrow(Object)
	 * @see #swim(int)
	 * @exception IllegalArgumentException
	 */
	public void insert(int key, T value) {
		if (contains(key))
			throw new IllegalArgumentException("index already exists; received: " + key);
		valueNotNullOrThrow(value);

		// Add to last node of the heap
		positionMap[key] = heapSize;
		inverseMap[heapSize] = key;
		values[key] = value;

		// Bubble(swim) up as needed
		swim(heapSize++);
	}

	/**
	 * Return the stored value at specified position of inserted values
	 * 
	 * @param key The specified position in the values array
	 * @return the associated value
	 * @see #keyExistsOrThrow(int)
	 */
	@SuppressWarnings("unchecked")
	public T valueOf(int key) {
		keyExistsOrThrow(key);
		return (T) values[key];
	}

	/**
	 * Remove a value from the heap and make sure it is balanced again
	 * 
	 * @param key The position of the value in the value array to be deleted
	 * @return The deleted value
	 * @see #keyExistsOrThrow(int)
	 * @see #swap(int, int)
	 * @see #sink(int)
	 * @see #swim(int)
	 */
	@SuppressWarnings("unchecked")
	public T delete(int key) {
		keyExistsOrThrow(key);
		final int i = positionMap[key];

		// swap deleted position with the last value
		swap(i, --heapSize);
		// Try to sink the new value
		sink(i);
		// Try to swim the new value
		swim(i);
		// heap is balanced again. Now it is safe to delete the unwanted value
		T value = (T) values[key];
		values[key] = null;
		positionMap[key] = -1;
		inverseMap[heapSize] = -1;
		return value;
	}

	/**
	 * Update is used to change the position value. After the change the heap is re
	 * balanced
	 * 
	 * @param key   The position of the value in the value array to be updated
	 * @param value The new value
	 * @return Old value
	 * @see #keyExistsAndValueNotNullOrThrow(int, Object)
	 * @see #sink(int)
	 * @see #swim(int)
	 */
	@SuppressWarnings("unchecked")
	public T update(int key, T value) {
		keyExistsAndValueNotNullOrThrow(key, value);
		final int i = positionMap[key];
		T oldValue = (T) values[key];
		values[key] = value;
		// Try to sink the new value
		sink(i);
		// Try to swim the new value
		swim(i);
		return oldValue;
	}

	/* Helper functions */

	/**
	 * Sink will try to swap the parent node with the minimal valued child node
	 * recursively. By seeking the minimal valued child we ensure the heap stayed
	 * balanced otherwise we risk getting a lower priority as the new parent. If
	 * there is no less valued child then the heap is balanced and no further
	 * sinking is needed
	 * 
	 * @param i parent's node index
	 * @see #lessValueChild(int)
	 * @see #swap(int, int)
	 */
	protected void sink(int i) {
		int j = lessValueChild(i);
		if (j != -1) {
			swap(i, j);
			// continue sinking
			sink(j);
		}
	}

	/**
	 * Swim will try to swap the child node with the parent node recursively as long
	 * as it has a higher priority(value is less then the parent's). If there is no
	 * more valued parent then the heap is balanced and no further swimming is
	 * needed
	 * 
	 * @param i child's node index
	 * @see #lessValueChild(int)
	 * @see #swap(int, int)
	 */
	protected void swim(int i) {
		int j = parent[i];
		if (less(i, j)) {
			swap(i, j);
			swim(j);
		}
	}

	/**
	 * From the parent node at index i, find the less valued child below it. Use
	 * previously calculated child position value from child's array. Notice that
	 * less valued means that its priority is higher since it is a minimal priority.
	 * If no less valued child is found then it means that the heap is balanced
	 * 
	 * @param i parent's node index
	 * @return minimum valued child position
	 * @see Math#min(int, int)
	 * @see #less(int, int)
	 */
	private int lessValueChild(int i) {
		int index = -1;
		int from = child[i];
		int to = min(heapSize, from + 2);

		for (int j = from; j < to; j++) {
			if (less(j, i)) {
				index = i = j;
			}
		}
		return index;
	}

	/**
	 * Swap node positions
	 * 
	 * @param i first node position
	 * @param j second node position
	 */
	private void swap(int i, int j) {
		positionMap[inverseMap[j]] = i;
		positionMap[inverseMap[i]] = j;
		int tmp = inverseMap[i];
		inverseMap[i] = inverseMap[j];
		inverseMap[j] = tmp;
	}

	/**
	 * Extract value objects for node i and node j and then see if node's i value is
	 * less then node's j. Value of i < value of j
	 * 
	 * @param i first node index
	 * @param j second node index
	 * @return true if value of first is less then the value of the second
	 * @see #less(Object, Object)
	 */
	private boolean less(int i, int j) {
		Object obj1 = values[inverseMap[i]];
		Object obj2 = values[inverseMap[j]];
		return less(i, obj1, j, obj2);
	}

	/**
	 * Tests if the value of obj1 is less then the value of obj2 Is obj1 < obj2
	 * 
	 * @param obj1 first value
	 * @param obj2 second value
	 * @return true if obj1 < obj2
	 * @see Comparable#compareTo(Object)
	 */
	@SuppressWarnings("unchecked")
	protected boolean less(int i, Object obj1, int j, Object obj2) {
		return ((Comparable<? super T>) obj1).compareTo((T) obj2) < 0;
	}

	/**
	 * Print the value keys in the order of the binary tree nodes
	 */
	@Override
	public String toString() {
		List<Integer> lst = new ArrayList<>(heapSize);
		for (int i = 0; i < heapSize; i++)
			lst.add(inverseMap[i]);
		return lst.toString();
	}

	/**
	 * From the parent node at index i, find the minimum valued child below it. Use
	 * previously calculated child position value from child's array. Notice that
	 * less valued means that its priority is higher since it is a minimal priority.
	 * 
	 * @param i parent's node index
	 * @return minimum valued child position
	 * @see Math#min(int, int)
	 * @see #less(int, int)
	 */
	private int minChild(int i) {
		int leftChild = child[i];
		int rightChild = child[leftChild+1];
		if (less(leftChild, rightChild)) {
			return leftChild;
		}
		return rightChild;
	}

	/**
	 * Throw an NoSuchElementException if the heap is empty
	 * 
	 * @exception NoSuchElementException
	 */
	private void isNotEmptyOrThrow() {
		if (isEmpty())
			throw new NoSuchElementException("Priority queue underflow");
	}

	/**
	 * Test if valid key and value
	 * 
	 * @param key   value position
	 * @param value the value
	 * @see #keyExistsOrThrow(int)
	 * @see #valueNotNullOrThrow(Object)
	 */
	private void keyExistsAndValueNotNullOrThrow(int key, Object value) {
		keyExistsOrThrow(key);
		valueNotNullOrThrow(value);
	}

	/**
	 * Test if heap contains this key
	 * 
	 * @exception NoSuchElementException
	 * @see {@link #contains(int)}
	 */
	private void keyExistsOrThrow(int key) {
		if (!contains(key))
			throw new NoSuchElementException("Index does not exist; received: " + key);
	}

	/**
	 * Test if value is not null otherwise throws an IllegalArgumentException
	 * 
	 * @exception IllegalArgumentException
	 */
	private void valueNotNullOrThrow(Object value) {
		if (value == null)
			throw new IllegalArgumentException("value cannot be null");
	}

	/**
	 * Test if key is in the heap bounds otherwise throws an
	 * IllegalArgumentException
	 * 
	 * @exception IllegalArgumentException
	 */
	private void keyInBoundsOrThrow(int key) {
		if (key < 0 || key >= maxHeapSize)
			throw new IllegalArgumentException("Key index out of bounds; received: " + key);
	}

	/**
	 * Heaped value polling iterator
	 * @author Madmon Tomer
	 *
	 */
	public class HeapIterator implements Iterator<T> {
		
		int index = 0;

		@Override
		public boolean hasNext() {
			return index < heapSize;
		}

		@Override
		public T next() {
			return valueOf(inverseMap[index++]);
		}

	}

	@Override
	public Iterator<T> iterator() {
		return new HeapIterator();
	}

}