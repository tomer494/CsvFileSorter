/**
 * 
 */
package com.ob1tech.CsvFileSorter.controllers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ob1tech.CsvFileSorter.algorithms.sort.MinIndexedBinaryHeap;
import com.ob1tech.CsvFileSorter.dateModel.RecordBatchNode;
import com.ob1tech.CsvFileSorter.dateModel.RecordIndex;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

/**
 * This class is the front control engine of the program. Its aim is
 * to read incoming file, preparing batches and sending for farther indexing
 * @author Madmon Tomer
 * @param <T> the type of the record key
 * @see Comparable
 */
public class Controller<T extends Comparable<T>>{
	
	/**
	 * Default buffer size. 5
	 */
	public static final int DEFAULT_BUFFER_SIZE = 5;
	
	/**
	 * Columns delimiter - comma
	 */
	private static final char COMMA_DELIMITER = ',';
	
	/**
	 * Working file path
	 */
	private Path filePath;
	/**
	 * batch size - how much files to boundl together
	 */
	private int batchSize;
	/**
	 * Key column position in the record
	 */
	private int keyIndex;
	/**
	 * Indicator of header row in the file
	 */
	private boolean skipHeader;
	/**
	 * Client selected key type
	 */
	private String keyType;
	/**
	 * records to batched map
	 */
	private Map<Long, Long> recordToBatchMap;

	/**
	 * Client selected key type class name
	 */
	private String keyDataType;

	/**
	 * follow line numbers
	 */
	private AtomicInteger nextLine;
	
	/**
	 * Batch controller instanse
	 */
	private BatchController<T> batchController;
	/**
	 * log time pass
	 */
	private long time;
	

	//logger
	public static final Logger logger = LogManager.getRootLogger();
	
	/**
	 * Constructor: Initioalization
	 * Will be build by the {@link ControllerBuilder}
	 */
	protected Controller() {
		nextLine = new AtomicInteger();
		recordToBatchMap = new HashMap<Long, Long>();
		
	}

	/**
	 * Start engine. reading and saving
	 * @see CSVReader
	 */
	public void execute() {
		time = System.currentTimeMillis();
		logger.info( "Start reading file..." );
        CSVReader csvReader = null;
		try {
			initBatchController(filePath, batchSize, keyDataType, recordToBatchMap);
			
			Reader reader = Files.newBufferedReader(filePath);
				    
			CSVParser parser = new CSVParserBuilder()
				    .withSeparator(COMMA_DELIMITER)
				    .build();
				 
			csvReader = new CSVReaderBuilder(reader)
				    .withSkipLines(skipHeader?1:0)
				    .withCSVParser(parser)
				    .build();
			
			
			boolean readMore = true;
			long batchId = 0l;
			do {
				/*
				 * read records
				 * heap records
				 * update indexes heap file
				 */
				//Read
				List<List<String>> records = readRecordBatch(csvReader, batchSize);
				readMore = records.size()==batchSize;
				
				if(!records.isEmpty()) {
					//Batch and sort
					RecordBatchNode<T> recordBatchNode = createRecordBatchNode(records, batchId++, batchSize);
					
					updateBatchController(recordBatchNode);
				}
				
			}while(readMore);
			logger.info("End reading file..."+(System.currentTimeMillis()-time)+" msc");
			
			finalizeBatchController();
			writeSortedFile();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(csvReader!=null) {
				try {
					csvReader.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

	/**
	 * Initialize a new batchController
	 * @param filePath
	 * @param batchSize
	 * @param keyDataType
	 * @param recordToBatchMap
	 */
	public void initBatchController(Path filePath, int batchSize, String keyDataType, Map<Long, Long> recordToBatchMap) {
		batchController = new BatchController<T>(filePath, batchSize, keyDataType, recordToBatchMap);
	}

	/**
	 * Call batchController write the sorted file
	 */
	public void writeSortedFile() {
		batchController.writeSortedFile();
		logger.info("Done sorting file! "+(System.currentTimeMillis()-time)+" msc");
	}

	/**
	 * reading the file and saving rows to batches
	 */
	public void finalizeBatchController() {
		batchController.finalizeBatchController(skipHeader);
	}

	/**
	 * Prepare sorted batch record batche nodes.
	 * Heapify by a priority index heap
	 * @param recordBatchNode
	 */
	protected void updateBatchController(RecordBatchNode<T> recordBatchNode) {
		logger.info("Add batch "+recordBatchNode.getId()+" at "+(System.currentTimeMillis()-time)+" msc");
		if(recordBatchNode.getRecords().size()>1) {
			//Read records in to the heap
			MinIndexedBinaryHeap<RecordIndex<T>> recordBatchNodeSorter = new MinIndexedBinaryHeap<RecordIndex<T>>(recordBatchNode.getRecords().size());
			for(RecordIndex<T> record:recordBatchNode.getRecords()) {
				recordBatchNodeSorter.insert(record);
			}
			//Reset the batch object
			recordBatchNode.getRecords().clear();
			//storring sorted
			while(!recordBatchNodeSorter.isEmpty()) {
				recordBatchNode.insert(recordBatchNodeSorter.pollMinValue());
			}
		}
		//sending batch to the controller
		batchController.insert(recordBatchNode);
	}

	/**
	 * Generating a new record batch.
	 * extracting the key by its type and line number
	 * @param records
	 * @param id
	 * @param batchSize
	 * @return
	 */
	private RecordBatchNode<T> createRecordBatchNode(List<List<String>> records, long id, int batchSize) {
		RecordBatchNode<T> recordsNode = new RecordBatchNode<T>(id);
		long lineNumber = id*batchSize;
		for(List<String> record : records) {
			recordToBatchMap.put(++lineNumber, id);
			String keyString = record.get( keyIndex );
			//Get kkey object by type name
			Object key;
			if(String.class.getTypeName().equals(keyDataType)) {
				key = keyString;
			}else if(Double.class.getTypeName().equals(keyDataType)) {
				key = Double.valueOf(keyString);
			}else if(Class.class.getTypeName().equals(keyDataType)) {
				if(NumberUtils.isCreatable(keyString)) {
					key = NumberUtils.createLong(keyString);
				}else {
					key = keyString;
				}
			}else {
				key = Long.valueOf(keyString);
			}
			
			@SuppressWarnings("unchecked")
			RecordIndex<T> recordNode = new RecordIndex<T>(lineNumber, (T) key);
			recordsNode.insert( recordNode  );
		}
		return recordsNode;
	}

	/**
	 * Read batch of records from working file
	 * @param csvReader
	 * @param batchSize
	 * @return list of row, separated to columns
	 * @throws IOException
	 */
	private List<List<String>> readRecordBatch(CSVReader csvReader, int batchSize) throws IOException {
		List<List<String>> records = new ArrayList<List<String>>();
		String[] values = null;
	    while ((values = csvReader.readNext()) != null) {
	    	nextLine.incrementAndGet();
	        records.add(Arrays.asList(values));
	        if( --batchSize == 0 ) {
	        	break;
	        }
	    }
		
		return records;
	}
	

	
	

	public Path getFilePath() {
		return filePath;
	}

	public void setFilePath(Path filePath) {
		this.filePath = filePath;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public int getKeyIndex() {
		return keyIndex;
	}

	public void setKeyIndex(int keyIndex) {
		this.keyIndex = keyIndex;
	}

	public boolean isSkipHeader() {
		return skipHeader;
	}

	public void setSkipHeader(boolean skipHeader) {
		this.skipHeader = skipHeader;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public void setKeyDataType(String keyDataType) {
		this.keyDataType = keyDataType;
		
	}

	public BatchController<T> getBatchController() {
		return batchController;
	}

	public void setBatchController(BatchController<T> batchController) {
		this.batchController = batchController;
	}
	
	public Map<Long, Long> getRecordToBatchMap() {
		return recordToBatchMap;
	}

	public void setRecordToBatchMap(Map<Long, Long> recordToBatchMap) {
		this.recordToBatchMap = recordToBatchMap;
	}

}
