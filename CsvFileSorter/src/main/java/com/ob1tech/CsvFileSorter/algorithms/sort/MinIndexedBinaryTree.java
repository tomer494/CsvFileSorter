package com.ob1tech.CsvFileSorter.algorithms.sort;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.dateModel.SortKey;


/**
 * <pre>
 * MinIndexedBinaryTree is an abstract class for implementing a binary tree form
 * to sort out index nodes and deal with collisions.
 * 
 * Nodes of the tree are not kept in memory.
 * left side of the tree will contain the lower values or the highest priority ones
 * and the right will contain the opposite.
 * 
 * </pre>
 * 
 * @author Madmon Tomer
 * 
 * @param <T> comparable value
 * @param <IndexNode<T>>
 * @see {@link Iterable}
 */
public abstract class MinIndexedBinaryTree<T> implements Iterable<T>{

	/**
	 * @Current number of elements in the tree.
	 */
	private AtomicLong treeSize;
	
	/**
	 * root is the head node of the binary tree
	 * @see IndexNode
	 */
	private IndexNode<T> root;

	private List<Long> lazyBalanceQeueu;

	/**
	 * logger
	 */
	Logger logger = LogManager.getLogger(MinIndexedBinaryTree.class);
	
	

	/**
	 * Contractor: Initiates the tree {@link AtomicLong} and a null root.
	 * 
	 */
	public MinIndexedBinaryTree() {

		treeSize = new AtomicLong();
		root = null;
		lazyBalanceQeueu = new LinkedList<Long>();
	}

	/**
	 * Get current tree size
	 * 
	 * @return long Current tree size
	 */
	public long size() {
		return treeSize.get();
	}

	/**
	 * insert a new value.
	 * The value will pass the nodes of the tree by comparison
	 * and create a leaf.
	 * 
	 * @param value Index node
	 * @see indexRecordController for implimentation
	 */
	public void add(IndexNode<T> value) {
		
		/**
		 * For debug purposes only. Do not use on large files, too expensive.
		 */
		if(logger.isDebugEnabled()) {
			printTree("Add: "+value);
		}
		
		/**
		 * prepare index node for saving.
		 * saving is an abstract method.
		 * @see indexRecordController for implimentation
		 */
		long nodeIndex = treeSize.getAndIncrement();
		value.setId(nodeIndex);
		save(nodeIndex, value);
		
		//init as root
		if(root==null) {
			root = value;
		}else {
			//get the head of the tree
			IndexNode<T> pointer = getRoot();
			//add element to tree
			add(pointer, value, nodeIndex, null);
		}
		
		/**
		 * For debug purposes only. Do not use on large files, too expensive.
		 */
		if(logger.isDebugEnabled()) {
			printTree("Post: ");
		}
		
	}

	protected void balance() {
		Set<Long> balanced = new HashSet<Long>();
		//Mast check root
		/*IndexNode<T> rootLeft = getRoot().getLeftNode()!=null?getValueOf(getRoot().getLeftNode()):null;
		IndexNode<T> rootRight = getRoot().getRightNode()!=null?getValueOf(getRoot().getRightNode()):null;
		update(getRoot(), rootLeft, rootRight);
		lazyBalanceQeueu.add(0,0l);*/
		Long nodeId = (lazyBalanceQeueu.size()>0)?lazyBalanceQeueu.remove(lazyBalanceQeueu.size()-1):null;
		while(nodeId!=null) {
			if(balanced.add(nodeId)) {
				IndexNode<T> newRoot = balance(nodeId);
				if(newRoot!=null && getRoot().getId()==nodeId) {
		    		setRoot(newRoot);
		    	}
			}
			if(lazyBalanceQeueu.size()>0) {
				nodeId = lazyBalanceQeueu.remove(lazyBalanceQeueu.size()-1);
			}else {
				nodeId = null;
			}
		}
		balanced.clear();
		balanced = null;
	}

	private IndexNode<T> balance(long nodeIndex) {
		IndexNode<T> node = getValueOf(nodeIndex);
		IndexNode<T> newParent = null;
	    
		IndexNode<T> leftNode = node.getLeftNode()!=null?getValueOf(node.getLeftNode()):null;
	    IndexNode<T> rightNode = node.getRightNode()!=null?getValueOf(node.getRightNode()):null;
	      
	    // Left heavy subtree.
	    long balanceLeft = leftNode!=null?leftNode.getBalance():0;
		long balanceRight = rightNode!=null?rightNode.getBalance():0;
		if (node.getBalance() <= -2) {

	      // Left-Left case.
	      if (Math.abs(balanceLeft) < balanceRight) {
	    	  newParent = leftLeftCase(node, leftNode, rightNode);

	        // Left-Right case.
	      } else {
	    	  newParent = leftRightCase(node, leftNode, rightNode);
	      }

	      // Right heavy subtree needs balancing.
	    } else if (node.getBalance() >= +2) {

	      // Right-Right case.
	      if (balanceRight > Math.abs(balanceLeft)) {
	    	  newParent = rightRightCase(node, leftNode, rightNode);

	        // Right-Left case.
	      } else {
	    	  newParent = rightLeftCase(node, leftNode, rightNode);
	      }
	    }

	    // Node either has a balance factor of 0, +1 or -1 which is fine.
	    return newParent;
		//Update root
		/*if(newParent!=null) {
			setRoot(newParent);
		}*/
	  }

	  private IndexNode<T> leftLeftCase(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
	    return rightRotation(node, leftNode, rightNode);
	  }

	  private IndexNode<T> leftRightCase(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
		  if(rightNode==null) {
			  return null;
		  }
		  IndexNode<T> leftLeftNode = 
				  leftNode.getLeftNode()!=null?
				  getValueOf(leftNode.getLeftNode()):null;
		  IndexNode<T> leftRightNode = 
				  leftNode.getRightNode()!=null?
				  getValueOf(leftNode.getRightNode()):null;
		  IndexNode<T> newLeft = leftRotation(leftNode,
				  leftLeftNode,
				  leftRightNode);
		  node.setLeftNode(newLeft!=null?newLeft.getId():null);
		  return leftLeftCase(node, newLeft, rightNode);
	  }

	  private IndexNode<T> rightRightCase(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
	    return leftRotation(node, leftNode, rightNode);
	  }

	  private IndexNode<T> rightLeftCase(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
		  if(rightNode==null) {
			  return null;
		  }
		  IndexNode<T> righLeftNode = 
				  rightNode.getLeftNode()!=null?
						  getValueOf(rightNode.getLeftNode()):null;
		  IndexNode<T> righRightNode = 
				  rightNode.getRightNode()!=null?
						  getValueOf(rightNode.getRightNode()):null;
		  IndexNode<T> newRight = rightRotation(rightNode,
				  righLeftNode,
				  righRightNode);
		node.setRightNode(newRight!=null?newRight.getId():null);
	    return rightRightCase(node, leftNode, newRight);
	  }

	  private IndexNode<T> leftRotation(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
		  if(rightNode==null) {
			  return null;
		  }
		  //logger.warn("ROTATE LEFT!");

		  printTree("ROTATE LEFT!");
		  IndexNode<T> newParent = rightNode;
	    node.setRightNode(newParent.getLeftNode());
	    newParent.setLeftNode(node.getId());
	    IndexNode<T> newRightNode = node.getRightNode()!=null?
	    		getValueOf(node.getRightNode()):null;
	    IndexNode<T> newParentRightNode = newParent.getRightNode()!=null?
	    		getValueOf(newParent.getRightNode()):null;
		update(node, leftNode, newRightNode);
	    update(newParent, node, newParentRightNode);
	    printTree("ROTATE LEFT!");
	    return newParent;
	  }

	  private IndexNode<T> rightRotation(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
		  if(leftNode==null) {
			  return null;
		  }
		  //logger.warn("ROTATE RIGHT!");
		  printTree("ROTATE RIGHT!");
		  IndexNode<T> newParent = leftNode;
		  node.setLeftNode(newParent.getRightNode());
		  newParent.setRightNode(node.getId());
		  IndexNode<T> newLeftNode = node.getLeftNode()!=null?
				  getValueOf(node.getLeftNode()):null;
		    IndexNode<T> newParentLeftNode = newParent.getLeftNode()!=null?
		    		getValueOf(newParent.getLeftNode()):null;
			update(node, newLeftNode, rightNode);
	    update(newParent, newParentLeftNode, node);
	    return newParent;
	  }
	  
	  private void update(IndexNode<T> node,
			  IndexNode<T> leftNode, IndexNode<T> rightNode) {
		 logger.info("update:"+node);
		 logger.info(leftNode+","+rightNode);
	    long leftNodeHeight = (leftNode == null) ? -1 : leftNode.getHeight();
	    long rightNodeHeight = (rightNode == null) ? -1 : rightNode.getHeight();

	    // Update this node's height.
	    node.setHeight(1 + Math.max(leftNodeHeight, rightNodeHeight));

	    // Update balance factor.
	    node.setBalance(rightNodeHeight - leftNodeHeight);
	    /*if(node.getId()==root.getId()) {
	    	System.out.println("WTF");
	    }*/
	    save(node.getId(), node);
	  }

	/**
	 * @return root
	 */
	public IndexNode<T> getRoot() {
		return root;
	}

	/**
	 * For debug purposes only. Do not use on large files, too expensive.
	 */	
	public void printTree(String title) {
		logger.info(title);
		
		@SuppressWarnings("rawtypes")
		Iterator it = iterator();
		while(it.hasNext()) {
			Object next = it.next();
			logger.info(next);
		}
		logger.trace("--------------");
	}
	
	/**
	 * Go to the pointers leaf node on the left, AKA higher value.
	 * If no leaf is there then the "value" node will park there, the id of the node.
	 * @param pointer node currently passing by
	 * @param value the node that is meant to enter or update the tree
	 * @param nodeIndex the id of the node, for saving purposes
	 * @param parentNode carry on parameter to indicate where where the last place we checked
	 * @return At best the leaf node or null if we got to the end of the road
	 * @see #goRight(IndexNode, IndexNode, Long, IndexNode)
	 */
	protected IndexNode<T> goLeft(IndexNode<T> pointer, IndexNode<T> value, Long nodeIndex, IndexNode<T> parentNode) {
		Long leafNodeIndex = pointer.getLeftNode();
		if(leafNodeIndex!=null) {
			pointer = getValueOf(leafNodeIndex);
			if(parentNode==null){
				updateNodeState(pointer, true);
			}
		}else {
			if(parentNode==null){
				pointer.setLeftNode(nodeIndex);
				updateNodeState(pointer, true, false);
				save(pointer.getId(), pointer);
			}
			return null;
		}
		return pointer;
	}
	
	private void updateNodeState(IndexNode<T> node, boolean left) {
		updateNodeState(node, left, true);
	}
	private void updateNodeState(IndexNode<T> node, boolean left, boolean doSave) {
		if(node.getLeftNode()!=null && left) {
			node.setHeight(node.getHeight()+1);
			node.setBalance(node.getBalance()-1);
		}else if(node.getRightNode()!=null) {
			node.setHeight(node.getHeight()+1);
			node.setBalance(node.getBalance()+1);			
		}else {
			return;
		}
		if(node.getBalance()>1 || node.getBalance()<1) {
			lazyBalanceQeueu.add(node.getId());
		}
		if(doSave) {
			save(node.getId(), node);
		}
	}

	/**
	 * Same as left but in reverse.
	 * @param pointer node currently passing by
	 * @param value the node that is meant to enter or update the tree
	 * @param nodeIndex the id of the node, for saving purposes
	 * @param parentNode carry on parameter to indicate where where the last place we checked
	 * @return At best the leaf node or null if we got to the end of the road
	 * @see #goLeft(IndexNode, IndexNode, Long, IndexNode)
	 */
	protected IndexNode<T> goRight(IndexNode<T> pointer, IndexNode<T> value, Long nodeIndex, IndexNode<T> parentNode) {
		Long leafNodeIndex = pointer.getRightNode();
		if(leafNodeIndex!=null) {
			pointer = getValueOf(leafNodeIndex);
			if(parentNode==null){
				updateNodeState(pointer, false);
			}
		}else {
			if(parentNode==null){
				pointer.setRightNode(nodeIndex);
				updateNodeState(pointer, false, false);
				save(pointer.getId(), pointer);
			}
			return null;
		}
		return pointer;
	}

	/**
	 * This is the main method for tumbling down the tree,
	 * in search of a nice parking leaf.
	 * Hare we choose where to go, left, right or between 
	 * @see {@link #doInnerSwap(IndexNode, IndexNode)}
	 * @param pointer node currently passing by
	 * @param value the node that is meant to enter or update the tree
	 * @param nodeIndex the id of the node, for saving purposes
	 * @param parentNode carry on parameter to indicate where where the last place we checked
	 * 
	 */
	protected void add(IndexNode<T> pointer, IndexNode<T> value, long nodeIndex, IndexNode<T> parentNode) {
		//List<Long> mixedValues = new ArrayList<Long>();
		IndexNode<T> root = pointer;
		while(pointer!=null) {
			
			logger.debug("pointer"+pointer.getKey()+",node"+value.getKey());
			IndexNode<T> prePointer = pointer;
			boolean goLeft = false;
			if(value.getKey().compareTo(pointer.getKey())<0) {
				//Go left
				pointer = goLeft(pointer, value, nodeIndex, parentNode);
				goLeft = true;
				//updateNodeState(prePointer, true);
			}else if(value.getKey().compareTo(pointer.getKey())>0) {
				//Go right
				pointer = goRight(pointer, value, nodeIndex, parentNode);
				//updateNodeState(prePointer, false);
			}else{
				//Can't decide, Handle it!
				//mixedValues.add(pointer.getId());
				pointer = handleMixedValues(pointer, value, nodeIndex, parentNode);
				//updateNodeState(prePointer, false);
				
			}
			if(parentNode==null && (pointer==null || prePointer.getId()!=pointer.getId())) {
				IndexNode<T> leftNode = prePointer.getLeftNode()!=null?getValueOf(prePointer.getLeftNode()):null;
			    IndexNode<T> rightNode = prePointer.getRightNode()!=null?getValueOf(prePointer.getRightNode()):null;
			    update(prePointer, leftNode, rightNode);
			    if(pointer!=null) {
			    	pointer = getValueOf(pointer.getId());
			    	if(prePointer.getId()==pointer.getId()) {
			    		setRoot(pointer);
			    	}
			    }
			    if(prePointer.getBalance()>1 || prePointer.getBalance()<1) {
					lazyBalanceQeueu.add(prePointer.getId());
				}
			}
		}
		/*		if(pointer.getLeftNode()!=null && value.getId()!=pointer.getId()) {
					//Pointer may be lower value/ higher priority so go left
					sendPointerToLeft(pointer, value, parentNode);
				}*/
		if(parentNode==null) {		
			balance();
		
			/*mixedValues.forEach(entry -> {
				Long leftNodeId = entry;
				IndexNode<T> leftNode = getValueOf(leftNodeId);
				if(leftNode.getLeftNode()!=null) {
					//Get actual value node from implementor(May be file)
					IndexNode<T> leftPointer = getValueOf(leftNode.getLeftNode());
					//Restart for sub tree
					add(leftPointer, leftNode, leftNode.getId(), leftNode);
				}
			});*/
		}

	}

	/**
	 * Since we do not support duplicates in records keys.
	 * We need to fix a comparison point. the values of our pointer node
	 * are mixed values with the intended value node. Please see {@link SortKey} for a better explanation
	 * Since we priorities left then after the swapping of values it is safe to continue right.
	 * pointer though needs to go left for re sorting, because we may have better its position from the mixed data
	 * @see SortKey
	 * @param pointer node currently passing by
	 * @param value the node that is meant to enter or update the tree
	 * @param nodeIndex the id of the node, for saving purposes
	 * @param parentNode carry on parameter to indicate where where the last place we checked
	 * @return return a right leaf
	 */
	protected IndexNode<T> handleMixedValues(IndexNode<T> pointer, IndexNode<T> value, long nodeIndex, IndexNode<T> parentNode) {
		doInnerSwap(value, pointer);
		//value is left with low valued, so go right
		if(pointer.getLeftNode()!=null) {
			sendPointerToLeft(pointer, value, parentNode);
		}
		return goRight(pointer, value, nodeIndex, parentNode);
	}

	/**
	 * Get the leaf values and go to {@link #add(IndexNode, IndexNode, long, IndexNode)}
	 * For re sorting
	 * @param pointer node currently passing by
	 * @param value the node that is meant to enter or update the tree
	 * @param parentNode carry on parameter to indicate where where the last place we checked
	 * 
	 */
	public void sendPointerToLeft(IndexNode<T> pointer, IndexNode<T> value, IndexNode<T> parentNode) {
		
		IndexNode<T> leftNode = pointer;
		//Get actual value node from implementor(May be file)
		IndexNode<T> leftPointer = getValueOf(leftNode.getLeftNode());
		//Restart for sub tree
		add(leftPointer, leftNode, leftNode.getId(), value);
			
	}
	
	public void setRoot(IndexNode<T> root) {
		this.root = root;
	}

	/**
	 * Implementor will decide what to do when persisting is required
	 * @param nodeIndex nodes id
	 * @param value node value
	 */
	protected abstract void save(long nodeIndex, IndexNode<T> value);

	/**
	 * Implementor will decide from where the node value should be injected
	 * @param nodeIndex node id
	 * @return the actual node
	 */
	protected abstract IndexNode<T> getValueOf(long nodeIndex);
	
	/**
	 * Implementor will decide what and how to handle mixed data nodes
	 * @param lowerLevelNode the intended lower leveled node
	 * @param higherLevelNode the intended heighr valued node
	 */
	protected abstract void doInnerSwap(IndexNode<T> lowerLevelNode, IndexNode<T> higherLevelNode);

	/**
	 * Tree value polling in order iterator.
	 * It uses a stack to save passed nodes for O(n) complexity
	 * @author Madmon Tomer
	 * @see Iterator
	 */
	public class MinIndexedBinaryTreeIterator implements Iterator<IndexNode<T>> {
		
		/**
		 * Start point for checking concurrency issue
		 */
		private long startAtHeapSize;
		/**
		 * by passing in the tree we need to carry on to the lowest point and then double back.
		 * In the stack we save id'e we have parsed for later printing
		 */
		private Stack<Long> nextIndexStack;
		private IndexNode<T> pointer;

		/**
		 * Contractor: of this iterator.
		 * Initiates the stack, start point and root element
		 * @param startAtHeapSize
		 */
		public MinIndexedBinaryTreeIterator(long startAtHeapSize) {
			this.startAtHeapSize = startAtHeapSize;
			nextIndexStack = new Stack<Long>();
			nextIndexStack.add(root.getId());
			pointer = root;
		}
		
		/*
		 * Check for concurrency issue by comparing start point and
		 * and current size of the tree.
		 * Will throw a ConcurrentModificationException
		 */
		private void noConcurencyUpdateOrThrow() {
			if(startAtHeapSize != treeSize.get()) {
				throw new ConcurrentModificationException();
			}
		}

		/**
		 * Check if has more elements to pass throw
		 * Will throw a ConcurrentModificationException
		 */
		public boolean hasNext() {
			noConcurencyUpdateOrThrow();
			return root!=null && !nextIndexStack.isEmpty();
		}

		/**
		 * Go get the next element to show
		 * Will throw a ConcurrentModificationException
		 */
		public IndexNode<T> next() {
			noConcurencyUpdateOrThrow();
			
			//Go deep left
			while(pointer!=null && pointer.getLeftNode()!=null) {
				//Collect id's on the way
				nextIndexStack.push(pointer.getLeftNode());
				//Get actual value node
				pointer = getValueOf(pointer.getLeftNode());
			}
			
			//Get actual value to show
			IndexNode<T> node = getValueOf(nextIndexStack.pop());
			
			//go right once. for next round
			if(node.getRightNode()!=null) {
				nextIndexStack.push(node.getRightNode());
				pointer = getValueOf(node.getRightNode());
			}
			
			return node;
		}

	}

	/**
	 * @return MinIndexedBinaryTreeIterator iterator
	 * @SuppressWarnings({ "unchecked", "rawtypes" })
	 */
	public Iterator iterator() {
		return new MinIndexedBinaryTreeIterator(size());
	}
	

}