package com.ob1tech.CsvFileSorter.controllers;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.ob1tech.CsvFileSorter.dateModel.IndexNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordBatchNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordIndex;
import com.ob1tech.CsvFileSorter.deserializer.RecordsNodeCustomDeserializer;
import com.ob1tech.CsvFileSorter.utils.Utilities;

/**
 * BatchController is responsible of arranging and managing batches of data records passed by
 * the {@link Controller}.
 * It will sort the batch. Save it and send it for indexing.
 * @author Madmon Tomer
 *
 * @param <T> key comparison type
 */
public class BatchController<T extends Comparable<T>> {
	
	/**
	 * An instance of the {@link IndexRecordController} thats manages the indexing
	 */
	private IndexRecordController<T> indexRecordController;

	/**
	 * Batch file saffix
	 */
	public static final String BATCH_SUFFIX = ".bch";

	/**
	 * Final sorted file prefix
	 */
	private static final String SORTED_PREFIX = "sorted_";
	
	/**
	 * The actual file path we work on
	 */
	private Path dataFile;
	/**
	 * This Map stores row to batch mappping, for a fast rereading of the file at the end
	 * of sorting. 
	 * @see finalizeBatchController
	 */
	private Map<Long, Long> recordToBatchMap;
	/**
	 * Header place holder, if head is present at the given file
	 */
	private String header;
	
	/**
	 * Count the incomming batches
	 */
	private AtomicLong batchCounter;
	
	/**
	 * Json desirializer mapping object of {@link RecordBatchNode}
	 */
	private ObjectMapper objectMapper = null;
	
	/**
	 * Json Deserializer - configures how is the text is decoded to object
	 * @see RecordsNodeCustomDeserializer implementation
	 */
	private JsonDeserializer<RecordBatchNode<T>> deserializer;
	
	/**
	 * logger
	 */
	Logger logger = LogManager.getLogger(BatchController.class);
	
	/**
	 * Constractor: Initialize BatchController instance.
	 * @param dataFile working file
	 * @param batchSize batch size
	 * @param keyType Type of the key, String, Long or double
	 * @param recordToBatchMap init the {@link #recordToBatchMap}
	 * @see initIndexRecordControler
	 */
	public BatchController(Path dataFile, int batchSize, 
			String keyType, Map<Long, Long> recordToBatchMap) {
		this.dataFile = dataFile;
		this.recordToBatchMap = recordToBatchMap;
		
		initIndexRecordControler(dataFile, batchSize, keyType);
		
		batchCounter = new AtomicLong();
		deserializer = new RecordsNodeCustomDeserializer<T>(RecordBatchNode.class, keyType);
	}

	/**
	 * Init a new {@link IndexRecordController}
	 * @param dataFile working file
	 * @param batchSize batch size
	 * @param keyType Type of the key, String, Long or double
	 * 
	 */
	public void initIndexRecordControler(Path dataFile, int batchSize, String keyType) {
		this.indexRecordController = new IndexRecordController<T>(this, dataFile, keyType, batchSize*2);
	}
	
	/**
	 * Json desirializer mapping object of {@link RecordBatchNode}
	 */
	public ObjectMapper getObjectMapper() {
		if(objectMapper==null) {
			objectMapper = new ObjectMapper();
			SimpleModule module =
					new SimpleModule("deserializer");
			module.addDeserializer(RecordBatchNode.class, deserializer);
			objectMapper.registerModule(module);
		}
		
		return objectMapper;
		
	}
	
	/**
	 * Saves the object to file for persistence
	 * @param nodeIndex batch id
	 * @param value {@link RecordBatchNode}
	 * @see Utilities
	 */
	protected void save(long nodeIndex, RecordBatchNode<T> value) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		Utilities.save(value, filePath, mapper);			
	}
	
	/**
	 * Get File path for a pacified batch id
	 * @param nodeIndex batch id
	 * @return path to relevant batch file
	 */
	private Path getFilePath(long nodeIndex) {
		return Utilities.resolve(dataFile.getParent(), 
				Utilities.constractFileName(dataFile, nodeIndex, BATCH_SUFFIX));
	}
	
	/**
	 * Gets the actual {@link RecordBatchNode} object from the file system.
	 * Using the Object maoper
	 * @SuppressWarnings("unchecked")
	 * @param nodeIndex
	 * @return
	 * @see #getObjectMapper()
	 * @see Utilities
	 */
	protected RecordBatchNode<T> getValueOf(long nodeIndex) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		return (RecordBatchNode<T>) Utilities.getValueOf(filePath, mapper , RecordBatchNode.class);
	}
	
	/**
	 * Insert is the entrance point of RecordBatchNode to be saved and proccesssed.
	 * Save the to file. Create an indexNode and send it to be indexesd by the {@link IndexRecordController}
	 * @param recordBatchNode batch object
	 * @see #save(long, RecordBatchNode)
	 * @see IndexRecordController
	 */
	public void insert(RecordBatchNode<T> recordBatchNode) {
		long time = System.currentTimeMillis();
		logger.info("Add batch "+recordBatchNode.getId()+" ...");
		
		batchCounter.getAndIncrement();
		save(recordBatchNode.getId(), recordBatchNode);
		IndexNode<T> indexNode = new IndexNode<T>(recordBatchNode.getId(), recordBatchNode.getKey());
		indexRecordController.add(indexNode);
		logger.info("End indexing batch "+recordBatchNode.getId()+" "+(System.currentTimeMillis()-time)+" msc");
		
	}
	
	/**
	 * Finalize batches, by reading the file again and placing the right record at the right batch.
	 * This will be di=one at the very end, after the batches have been sorted.
	 * @param skipHeader indicator of a header in the file
	 */
	public void finalizeBatchController(boolean skipHeader) {
		long time = System.currentTimeMillis();
		logger.info("Reading file and saving lines to batches...");
		BufferedReader reader;
		try {
			reader = Files.newBufferedReader(dataFile);
			String record = reader.readLine();
			if(record!=null && skipHeader) {
				header = record;
				record = reader.readLine();
			}
			long lineNumber = 0;
			while(record!=null) {
				lineNumber++;
				//get the relevant batch file id
				Long batchId = recordToBatchMap.get(lineNumber);
				//Get actual RecordBatchNode
				RecordBatchNode<T> batchNode = getValueOf(batchId);
				final long lineNum = lineNumber;
				//Get from within the batch the right record index
				RecordIndex<T> recordIndex = batchNode.getRecords().stream().filter(entry ->
					entry.getRecordLine()==lineNum).findAny().get();
				//Set the actual line
				recordIndex.setRecord(record);
				//Re save
				save(batchId, batchNode);
				//read next recourd
				record = reader.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("End reading file and saving lines to batches..."+(System.currentTimeMillis()-time)+" msc");
		
	}

	/**
	 * This method will iterate throw the index records
	 * and compose the sorted version of the file
	 * @SuppressWarnings({ "unchecked", "rawtypes" })
	 */
	public void writeSortedFile() {
		long time = System.currentTimeMillis();
		logger.info("Start write sorted file...");
		//Prepare sorted file name
		String fileName = SORTED_PREFIX+dataFile.getFileName().toString();
		Path sortedFile = Utilities.resolve(dataFile.getParent(), fileName);
		FileOutputStream fop = null;
		try {
			//Clear history
			Files.deleteIfExists(sortedFile);
			Files.createFile(sortedFile);
			//write header to new file
			fop = new FileOutputStream(sortedFile.toFile(),true);
		    if(header!=null) {
		    	fop.write(header.getBytes());
		    	fop.write(System.getProperty("line.separator").getBytes());
		    }
		    //Iterate on index files
			Iterator iterator = indexRecordController.iterator();
			while(iterator.hasNext()) {
				//Get sorted index file
				IndexNode<T> indexNode = (IndexNode<T>) iterator.next();
				//Get sorrted RecordBatchNodes
				RecordBatchNode<T> batchNode = getValueOf(indexNode.getId());
				//Iterate on records
				Iterator<RecordIndex<T>> recordsNodeiterator = batchNode.getRecords().iterator();
				//write to sorted file
				while(recordsNodeiterator.hasNext()) {
					fop.write(recordsNodeiterator.next().getRecord().getBytes());
					fop.write(System.getProperty("line.separator").getBytes());
				}
			}
	    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			//Close file
			if(fop!=null) {
			    try {
			    	fop.flush();
					fop.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		logger.info("Done write sorted file! "+(System.currentTimeMillis()-time)+" msc");
	}

	/**
	 * While sorting the batch controller will be called to fix the changed row to batch position
	 * @param recordLine
	 * @param id batch id
	 */
	public void updateRecordPosition(long recordLine, Long id) {
		recordToBatchMap.put(recordLine, id);		
	}

	/**
	 * return the current batch created
	 * @return number of batches
	 */
	public long getBatchCounter() {
		return batchCounter.get();
	}

	/**
	 * return the IndexRecordController instance
	 * @return IndexRecordController
	 * @see IndexRecordController
	 */
	public IndexRecordController<T> getIndexRecordController() {
		return indexRecordController;
	}

	/**
	 * Enable setting of customized {@link IndexRecordController}
	 * @param indexRecordController
	 */
	public void setIndexRecordController(IndexRecordController<T> indexRecordController) {
		this.indexRecordController = indexRecordController;
	}
	
}
