package com.ob1tech.CsvFileSorter.controllers;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.ob1tech.CsvFileSorter.algorithms.sort.MinIndexedBinaryTree;
import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordBatchNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordIndex;
import com.ob1tech.CsvFileSorter.deserializer.IndexNodeCustomDeserializer;
import com.ob1tech.CsvFileSorter.utils.Utilities;

public class IndexRecordController<T extends Comparable<T>>
	extends MinIndexedBinaryTree<T> 
	{
	
	String PRINT_SWAP_FORMAT = "%s swap:\t H#%s%s,L#%s%s";
	
	Logger logger = LogManager.getLogger(IndexRecordController.class);
	
	@SuppressWarnings("rawtypes")
	private JsonDeserializer<IndexNode<T>> deserializerInstance;
	private ObjectMapper objectMapper = null;
	
	private BatchController<T> batchController;
	
	private List<SimpleEntry<Long, IndexNode<T>>> inMemoryNodeQueue;
	private int maxInMemoryNodes;
	
	//public static final String BATCH_SUFFIX = ".bch";
	public static final String INDEX_SUFFIX = ".ind";
	
	private Path dataFile;
	
	public IndexRecordController(BatchController<T> batchController, Path dataFile, String keyType, int maxInMemoryNodes) {
		this.dataFile = dataFile;
		this.batchController = batchController;
		
		deserializerInstance = new IndexNodeCustomDeserializer<>(IndexNode.class, keyType);
		
		inMemoryNodeQueue = new LinkedList<AbstractMap.SimpleEntry<Long,IndexNode<T>>>();
		this.maxInMemoryNodes = maxInMemoryNodes;
		
	}
	

	public ObjectMapper getObjectMapper() {
		if(objectMapper==null) {
			objectMapper = new ObjectMapper();
			SimpleModule module =
					new SimpleModule("deserializer");
			module.addDeserializer(IndexNode.class, deserializerInstance);
			objectMapper.registerModule(module);
		}
		
		return objectMapper;
		
	}
	
	/*private IndexNode<T> nodeCache(long nodeIndex) {
		Optional<SimpleEntry<Long, IndexNode<T>>> any = 
				inMemoryNodeQueue.stream().filter(entry -> entry.getKey().equals(nodeIndex)).findAny();
		if(any.isPresent()) {
			return any.get().getValue();
		}
		return null;
	}
	
	private void putToCache(long nodeIndex, IndexNode<T> indexNode) {
		SimpleEntry<Long, IndexNode<T>> element = 
				new SimpleEntry<Long, IndexNode<T>>(nodeIndex, indexNode);
		inMemoryNodeQueue.add(0, element );
		if(inMemoryNodeQueue.size()>maxInMemoryNodes) {
			inMemoryNodeQueue.remove(maxInMemoryNodes);
		}
		
	}*/
	
	private Path getFilePath(long nodeIndex) {
		return Utilities.resolve(dataFile.getParent(), 
				Utilities.constractFileName(dataFile, nodeIndex, INDEX_SUFFIX));
	}
	
	protected void save(long nodeIndex, IndexNode<T> value) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		Utilities.save(value, filePath, mapper);			
	}

	@SuppressWarnings("unchecked")
	@Override
	protected IndexNode<T> getValueOf(long nodeIndex) {
		IndexNode<T> indexNode/* = nodeCache(nodeIndex)*/;
		/*if(indexNode!=null) {
			return indexNode;
		}else {*/
			Path filePath = getFilePath(nodeIndex);
			ObjectMapper mapper = getObjectMapper();
			indexNode = (IndexNode<T>) Utilities.getValueOf(filePath, mapper , IndexNode.class);
			/*putToCache(nodeIndex, indexNode);*/
		/*}*/
		return indexNode;
	}


	@Override
	protected void doInnerSwap(IndexNode<T> lowerLevelNode, IndexNode<T> higherLevelNode) {
		RecordBatchNode<T> lowerLevelRecordBatchNode = batchController.getValueOf(lowerLevelNode.getId());
		List<RecordIndex<T>> lowerLevelNodeRecords = lowerLevelRecordBatchNode.getRecords();
		
		RecordBatchNode<T> higherLevelRecordBatchNode = batchController.getValueOf(higherLevelNode.getId());
		List<RecordIndex<T>> higherLevelNodeRecords = higherLevelRecordBatchNode.getRecords();
		int higherLevelNodeRecordsSize = higherLevelNodeRecords.size();
		
		String swapMessage = String.format(PRINT_SWAP_FORMAT,
				"Pre",
				higherLevelNode.getId(),
				higherLevelNodeRecords,
				lowerLevelNode.getId(),
				lowerLevelNodeRecords);
		logger.debug(swapMessage);
		
		int highrIndex = 0, lowerIndex = 0;
		List<RecordIndex<T>> sortedList = new LinkedList<RecordIndex<T>>();
		while(highrIndex < higherLevelNodeRecords.size()) {
			RecordIndex<T> record = higherLevelNodeRecords.get(highrIndex);
			while(lowerIndex < lowerLevelNodeRecords.size()) {
				RecordIndex<T> recordLow = lowerLevelNodeRecords.get(lowerIndex);
				if(recordLow.compareTo(record)<0) {
					sortedList.add(recordLow);
					lowerIndex++;
				}else {
					break;
				}
			}
			sortedList.add(record);
			highrIndex++;
		}
		//Add the rest
		while(lowerIndex < lowerLevelNodeRecords.size()) {
			RecordIndex<T> recordLow = lowerLevelNodeRecords.get(lowerIndex);
			sortedList.add(recordLow);
			lowerIndex++;			
		}
		
		lowerLevelRecordBatchNode.reset();
		higherLevelRecordBatchNode.reset();
		
		for(RecordIndex<T> recordIndex : sortedList) {
			if(higherLevelRecordBatchNode.getRecords().size()<higherLevelNodeRecordsSize) {
				higherLevelRecordBatchNode.insert(recordIndex);
				batchController.updateRecordPosition(recordIndex.getRecordLine(),higherLevelRecordBatchNode.getId());
			}else {
				lowerLevelRecordBatchNode.insert(recordIndex);
				batchController.updateRecordPosition(recordIndex.getRecordLine(),lowerLevelRecordBatchNode.getId());
			}
		}
		
		persistChange(lowerLevelNode, lowerLevelRecordBatchNode);
		persistChange(higherLevelNode, higherLevelRecordBatchNode);
		
		swapMessage = String.format(PRINT_SWAP_FORMAT ,
				"Post",
				higherLevelNode.getId(),
				higherLevelNodeRecords,
				lowerLevelNode.getId(),
				lowerLevelNodeRecords);
		logger.debug(swapMessage);
		
	}


	public void persistChange(IndexNode<T> indexNode, RecordBatchNode<T> recordBatchNode) {
		indexNode.getKey().setMinValue(recordBatchNode.getKey().getMinValue());
		indexNode.getKey().setMaxValue(recordBatchNode.getKey().getMaxValue());
		save(indexNode.getId(), indexNode);
		batchController.save(recordBatchNode.getId(), recordBatchNode);
	}
	
}
