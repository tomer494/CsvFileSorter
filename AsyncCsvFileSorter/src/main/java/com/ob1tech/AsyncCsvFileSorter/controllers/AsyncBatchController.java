package com.ob1tech.AsyncCsvFileSorter.controllers;

import java.nio.file.Path;
import java.util.Map;

import com.ob1tech.CsvFileSorter.controllers.BatchController;

/**
 * Async extention of BatchController
 * injects an AsyncIndexRecordController to the origional BatchController
 * @author Madmon Tomer
 *
 * @param <T>
 * @see BatchController
 * @see AsyncIndexRecordController
 */
public class AsyncBatchController<T extends Comparable<T>> extends BatchController<T> {

	@Override
	public void initIndexRecordControler(Path dataFile, int batchSize, String keyType) {
		setIndexRecordController(new AsyncIndexRecordController<T>(this, dataFile, keyType, batchSize*2));
	}

	public AsyncBatchController(Path dataFile, int batchSize, String keyType, Map<Long, Long> recordToBatchMap) {
		super(dataFile, batchSize, keyType, recordToBatchMap);
		// TODO Auto-generated constructor stub
	}

}
