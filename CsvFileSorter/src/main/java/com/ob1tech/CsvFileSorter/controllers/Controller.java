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
 * @author Madmon Tomer
 *
 */
public class Controller<T extends Comparable<T>>{
	
	public static final int DEFAULT_BUFFER_SIZE = 5;
	
	private static final char COMMA_DELIMITER = ',';
	
	private Path filePath;
	private int batchSize;
	private int keyIndex;
	private boolean skipHeader;
	//private String header;
	private String keyType;
	private Map<Long, Long> recordToBatchMap;

	private String keyDataType;

	private AtomicInteger nextLine;
	private BatchController<T> batchController;
	private long time;
	

	public static final Logger logger = LogManager.getRootLogger();
	
	protected Controller() {
		nextLine = new AtomicInteger();
		recordToBatchMap = new HashMap<Long, Long>();
		
	}

	public Logger getLog() {
		return logger;
	}

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
				List<List<String>> records = readRecordBatch(csvReader, batchSize);
				readMore = records.size()==batchSize;
				
				if(!records.isEmpty()) {
					
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

	public void initBatchController(Path filePath, int batchSize, String keyDataType, Map<Long, Long> recordToBatchMap) {
		batchController = new BatchController<T>(filePath, batchSize, keyDataType, recordToBatchMap);
	}

	public void writeSortedFile() {
		batchController.writeSortedFile();
		logger.info("Done sorting file! "+(System.currentTimeMillis()-time)+" msc");
	}

	public void finalizeBatchController() {
		batchController.finalizeBatchController(skipHeader);
	}

	protected void updateBatchController(RecordBatchNode<T> recordBatchNode) {
		logger.info("Add batch "+recordBatchNode.getId()+" at "+(System.currentTimeMillis()-time)+" msc");
		if(recordBatchNode.getRecords().size()>1) {
			MinIndexedBinaryHeap<RecordIndex<T>> recordBatchNodeSorter = new MinIndexedBinaryHeap<RecordIndex<T>>(recordBatchNode.getRecords().size());
			for(RecordIndex<T> record:recordBatchNode.getRecords()) {
				recordBatchNodeSorter.insert(record);
			}
			recordBatchNode.getRecords().clear();
			while(!recordBatchNodeSorter.isEmpty()) {
				recordBatchNode.insert(recordBatchNodeSorter.pollMinValue());
			}
		}
		batchController.insert(recordBatchNode);
	}

	private RecordBatchNode<T> createRecordBatchNode(List<List<String>> records, long id, int batchSize) {
		RecordBatchNode<T> recordsNode = new RecordBatchNode<T>(id);
		long lineNumber = id*batchSize;
		for(List<String> record : records) {
			recordToBatchMap.put(++lineNumber, id);
			String keyString = record.get( keyIndex );
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
			
			/*String recordString = record.stream().map(String::valueOf) 
            .collect(Collectors.joining(","));*/
			@SuppressWarnings("unchecked")
			RecordIndex<T> recordNode = new RecordIndex<T>(lineNumber, (T) key);
			recordsNode.insert( recordNode  );
		}
		return recordsNode;
	}

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
