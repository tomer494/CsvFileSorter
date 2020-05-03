package com.ob1tech.CsvFileSorter.dateModel;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class represents an index node data model
 * @author Madmon Tomer
 *
 * @param <T>
 */
public class IndexNode<T> extends AbstractDataNode<T> implements Serializable{
	

	@JsonProperty("leftNode")
	private Long leftNode;
	@JsonProperty("rightNode")
	private Long rightNode;
	
	
	
	@Override
	public String toString() {
		String left = leftNode==null?"":leftNode.toString();
		String right = rightNode==null?"":rightNode.toString();
		return "#"+getId()+getKey()+"[l:" + left + ", r:" + right + "]";
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 5965463679057471522L;
	
	public IndexNode(Long id, SortKey<T> key) {
		super(key, id);
		this.leftNode = null;
		this.rightNode = null;
	}
	
	public IndexNode(Long id, SortKey<T> key, Long leftNode, Long rightNode) {
		super(key, id);
		this.leftNode = leftNode;
		this.rightNode = rightNode;
	}

	
	public Long getLeftNode() {
		return leftNode;
	}

	public void setLeftNode(Long leftNode) {
		this.leftNode = leftNode;
	}

	public Long getRightNode() {
		return rightNode;
	}

	public void setRightNode(Long rightNode) {
		this.rightNode = rightNode;
	}
	
}
