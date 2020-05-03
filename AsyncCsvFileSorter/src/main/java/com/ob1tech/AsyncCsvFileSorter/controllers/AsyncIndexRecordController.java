package com.ob1tech.AsyncCsvFileSorter.controllers;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ob1tech.CsvFileSorter.controllers.BatchController;
import com.ob1tech.CsvFileSorter.controllers.IndexRecordController;
import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.utils.Utilities;

public class AsyncIndexRecordController<T extends Comparable<T>> extends IndexRecordController<T> {

	private Object lock,lock2;
	private Utilities lockUtilities;
	private Map<Long, Boolean> treeLockList;
	private Logger logger = LogManager.getLogger(AsyncIndexRecordController.class);
	

	public AsyncIndexRecordController(BatchController<T> batchController, Path dataFile, String keyType,
			int maxInMemoryNodes) {
		super(batchController, dataFile, keyType, maxInMemoryNodes);
		lock = new Object();
		lock2 = new Object();
		lockUtilities = new Utilities();
		treeLockList = new ConcurrentHashMap<Long, Boolean>();
	}
	
	protected void add(IndexNode<T> pointer, IndexNode<T> value, long nodeIndex, IndexNode<T> parentNode) {
		//Long treeHead = pointer.getId();
		if(pointer.getId()==0l) {
			synchronized (lock) {
				while(treeLockList.getOrDefault(0l, false)) {
					try {
						lock.wait(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				treeLockList.put(0l, true);
			}
		}
		Long preSubTreeHead = pointer.getId();
		Long subTreeHead = pointer.getId();
		while(pointer!=null) {
			subTreeHead = pointer.getId();
			synchronized (lock2) {
				while(preSubTreeHead != subTreeHead && treeLockList.getOrDefault(subTreeHead, false)) {
					try {
						lock2.wait(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				treeLockList.put(subTreeHead, true);
			}
			logger.debug("pointer"+pointer.getKey()+",node"+value.getKey());
			if(value.getKey().compareTo(pointer.getKey())<0) {
				pointer = goLeft(pointer, value, nodeIndex, parentNode);
			}else if(value.getKey().compareTo(pointer.getKey())>0) {
				pointer = goRight(pointer, value, nodeIndex, parentNode);
			}else{
				pointer = handleMixedValues(pointer, value, nodeIndex, parentNode);		
			}
			if(preSubTreeHead != subTreeHead) {				
				treeLockList.put(preSubTreeHead, false);
			}
			preSubTreeHead = subTreeHead;
		}
		treeLockList.remove(subTreeHead);
		treeLockList.remove(0l);

	}

}
