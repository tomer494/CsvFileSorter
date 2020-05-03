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

/**
 * A record sorting implementation of a {@link MinIndexedBinaryTree}.
 * Deal with persisstence and handling collisssions.
 * When key are mixed for tow batches and some belong to the other batch, then we will
 * nead to sort it out at {@link #doInnerSwap(IndexNode, IndexNode)}
 * @author Madmon Tomer
 *
 * @param <T> ket comparable type used for sorting
 * @see MinIndexedBinaryTree
 * @see Comparable
 */
public class IndexRecordController<T extends Comparable<T>>
	extends MinIndexedBinaryTree<T> 
	{
	
	//loggging message format
	private String PRINT_SWAP_FORMAT = "%s swap:\t H#%s%s,L#%s%s";
	
	private Logger logger = LogManager.getLogger(IndexRecordController.class);
	
	/**
	 * IndexNode deserializer. Used to deserializ json string to object
	 * @SuppressWarnings("rawtypes")
	 */
	private JsonDeserializer<IndexNode<T>> deserializerInstance;
	/**
	 * Json mapper
	 */
	private ObjectMapper objectMapper = null;
	
	/**
	 * Link to the owning batch controller
	 */
	private BatchController<T> batchController;
	
	public static final String INDEX_SUFFIX = ".ind";
	
	private Path dataFile;
	
	/**
	 * Constractor:
	 * Tinitalize the working file, batch controller and a new IndexNodeCustomDeserializer
	 * @param batchController
	 * @param dataFile
	 * @param keyType
	 * @param maxInMemoryNodes
	 * @see IndexNodeCustomDeserializer
	 * @see BatchController
	 * @see IndexNode
	 */
	public IndexRecordController(BatchController<T> batchController, Path dataFile, String keyType, int maxInMemoryNodes) {
		this.dataFile = dataFile;
		this.batchController = batchController;
		
		deserializerInstance = new IndexNodeCustomDeserializer<>(IndexNode.class, keyType);
		
	}
	

	/**
	 * Json mapper
	 * @return Json mapper object
	 */
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
	
	/**
	 * Create index file path
	 * @param nodeIndex index file Id
	 * @return index file path
	 */
	private Path getFilePath(long nodeIndex) {
		return Utilities.resolve(dataFile.getParent(), 
				Utilities.constractFileName(dataFile, nodeIndex, INDEX_SUFFIX));
	}
	
	/**
	 * IndexNode perssister. Saves to file.
	 */
	protected void save(long nodeIndex, IndexNode<T> value) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		Utilities.save(value, filePath, mapper);
	}

	/**
	 * Return an object instance of IndexNode type by it's id
	 * @param nodeIndex index id
	 * @return IndexNode object
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected IndexNode<T> getValueOf(long nodeIndex) {
		IndexNode<T> indexNode;
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		indexNode = (IndexNode<T>) Utilities.getValueOf(filePath, mapper , IndexNode.class);
		return indexNode;
	}


	/**
	 * Inner swap is needed when batches mix keys. This methode will sort them out.
	 * The lists of keys in each RecordBatchNode are sorted so will pass them one 
	 * after the other and set them to a new ordered list.
	 * Then will reset the tow batches and the intended higher ranking one will be populated
	 * with lower values and the rest will go to the lower ranking batch.
	 * Indexes will be updated aswell and send back for further sorting.
	 * 
	 * @param lowerLevelNode the intended lower leveled node
	 * @param higherLevelNode the intended heighr valued node
	 */
	@Override
	protected void doInnerSwap(IndexNode<T> lowerLevelNode, IndexNode<T> higherLevelNode) {
		//Retrieve batches
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
		
		//Prepare full ordered list
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
		
		//Reset batches
		lowerLevelRecordBatchNode.reset();
		higherLevelRecordBatchNode.reset();
		
		//Populate orderedd
		for(RecordIndex<T> recordIndex : sortedList) {
			if(higherLevelRecordBatchNode.getRecords().size()<higherLevelNodeRecordsSize) {
				higherLevelRecordBatchNode.insert(recordIndex);
				batchController.updateRecordPosition(recordIndex.getRecordLine(),higherLevelRecordBatchNode.getId());
			}else {
				lowerLevelRecordBatchNode.insert(recordIndex);
				batchController.updateRecordPosition(recordIndex.getRecordLine(),lowerLevelRecordBatchNode.getId());
			}
		}
		
		//Persist
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


	/**
	 * Save indexes and batches back to their files
	 * @param indexNode
	 * @param recordBatchNode
	 */
	public void persistChange(IndexNode<T> indexNode, RecordBatchNode<T> recordBatchNode) {
		indexNode.getKey().setMinValue(recordBatchNode.getKey().getMinValue());
		indexNode.getKey().setMaxValue(recordBatchNode.getKey().getMaxValue());
		save(indexNode.getId(), indexNode);
		batchController.save(recordBatchNode.getId(), recordBatchNode);
	}
	
}
