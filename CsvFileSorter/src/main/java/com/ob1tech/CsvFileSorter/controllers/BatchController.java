package com.ob1tech.CsvFileSorter.controllers;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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

public class BatchController<T extends Comparable<T>> {
	
	private IndexRecordController<T> indexRecordController;

	public static final String BATCH_SUFFIX = ".bch";
	//public static final String INDEX_SUFFIX = ".ind";

	private static final String SORTED_PREFIX = "sorted_";
	
	private Path dataFile;
	private Map<Long, Long> recordToBatchMap;
	private String header;
	
	private AtomicLong batchCounter;
	
	private ObjectMapper objectMapper = null;
	private JsonDeserializer<RecordBatchNode<T>> deserializer;
	
	Logger logger = LogManager.getLogger(BatchController.class);
	
	public BatchController(Path dataFile, int batchSize, 
			String keyType, Map<Long, Long> recordToBatchMap) {
		this.dataFile = dataFile;
		this.recordToBatchMap = recordToBatchMap;
		
		initIndexRecordControler(dataFile, batchSize, keyType);
		
		batchCounter = new AtomicLong();
		deserializer = new RecordsNodeCustomDeserializer<T>(RecordBatchNode.class, keyType);
	}

	public void initIndexRecordControler(Path dataFile, int batchSize, String keyType) {
		this.indexRecordController = new IndexRecordController<T>(this, dataFile, keyType, batchSize*2);
	}
	
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
	
	protected void save(long nodeIndex, RecordBatchNode<T> value) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		Utilities.save(value, filePath, mapper);			
	}
	
	private Path getFilePath(long nodeIndex) {
		return Utilities.resolve(dataFile.getParent(), 
				Utilities.constractFileName(dataFile, nodeIndex, BATCH_SUFFIX));
	}
	
	@SuppressWarnings("unchecked")
	protected RecordBatchNode<T> getValueOf(long nodeIndex) {
		Path filePath = getFilePath(nodeIndex);
		ObjectMapper mapper = getObjectMapper();
		return (RecordBatchNode<T>) Utilities.getValueOf(filePath, mapper , RecordBatchNode.class);
	}
	
	public void insert(RecordBatchNode<T> recordBatchNode) {
		long time = System.currentTimeMillis();
		logger.info("Add batch "+recordBatchNode.getId()+" ...");
		
		batchCounter.getAndIncrement();
		save(recordBatchNode.getId(), recordBatchNode);
		IndexNode<T> indexNode = new IndexNode<T>(recordBatchNode.getId(), recordBatchNode.getKey());
		indexRecordController.add(indexNode);
		logger.info("End indexing batch "+recordBatchNode.getId()+" "+(System.currentTimeMillis()-time)+" msc");
		
	}
	
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
				Long batchId = recordToBatchMap.get(lineNumber);
				RecordBatchNode<T> batchNode = getValueOf(batchId);
				final long lineNum = lineNumber;
				RecordIndex<T> recordIndex = batchNode.getRecords().stream().filter(entry ->
					entry.getRecordLine()==lineNum).findAny().get();
				recordIndex.setRecord(record);
				save(batchId, batchNode);
				record = reader.readLine();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		logger.info("End reading file and saving lines to batches..."+(System.currentTimeMillis()-time)+" msc");
		
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void writeSortedFile() {
		long time = System.currentTimeMillis();
		logger.info("Start write sorted file...");
		String fileName = SORTED_PREFIX+dataFile.getFileName().toString();
		Path sortedFile = Utilities.resolve(dataFile.getParent(), fileName);
		FileOutputStream fop = null;
		try {
			Files.deleteIfExists(sortedFile);
			Files.createFile(sortedFile);
		
			fop = new FileOutputStream(sortedFile.toFile(),true);
		    if(header!=null) {
		    	fop.write(header.getBytes());
		    	fop.write(System.getProperty("line.separator").getBytes());
		    }
			Iterator iterator = indexRecordController.iterator();
			while(iterator.hasNext()) {
				IndexNode<T> indexNode = (IndexNode<T>) iterator.next();
				RecordBatchNode<T> batchNode = getValueOf(indexNode.getId());
				Iterator<RecordIndex<T>> recordsNodeiterator = batchNode.getRecords().iterator();
				while(recordsNodeiterator.hasNext()) {
					fop.write(recordsNodeiterator.next().getRecord().getBytes());
					fop.write(System.getProperty("line.separator").getBytes());
				}
			}
	    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
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

	public void updateRecordPosition(long recordLine, Long id) {
		recordToBatchMap.put(recordLine, id);		
	}

	public long getBatchCounter() {
		return batchCounter.get();
	}

	public IndexRecordController<T> getIndexRecordController() {
		return indexRecordController;
	}

	public void setIndexRecordController(IndexRecordController<T> indexRecordController) {
		this.indexRecordController = indexRecordController;
	}
	
}
