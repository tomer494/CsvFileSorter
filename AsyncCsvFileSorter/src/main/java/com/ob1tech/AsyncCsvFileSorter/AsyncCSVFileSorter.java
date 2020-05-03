package com.ob1tech.AsyncCsvFileSorter;

import com.ob1tech.AsyncCsvFileSorter.controllers.AsyncController;
import com.ob1tech.AsyncCsvFileSorter.controllers.AsyncControllerBuilder;
import com.ob1tech.CsvFileSorter.CSVFileSorter;

/**
 * Async CsvFileSorter version
 * @author Madmon Tomer
 * @see CSVFileSorter
 */
public class AsyncCSVFileSorter extends CSVFileSorter {
	
	public static void main( String[] args )
    {
		AsyncCSVFileSorter asyncCSVFileSorter = new AsyncCSVFileSorter();
		
		asyncCSVFileSorter.init(args);
		asyncCSVFileSorter.run(skipHead, filePath, keyIndex, bufferSize, keyType);
				
    	System.exit(0);
    }

	@Override
	protected void run(byte skipHead, String filePath, int keyIndex, int bufferSize, String keyType) {
		System.out.println( "Start indexing file..." );
        
		AsyncControllerBuilder builder = new AsyncControllerBuilder(filePath);
    	
    	builder.withSkipHeader(skipHead>0);
    	
		builder.withKeyIndex(keyIndex );
		builder.withBatchSize( bufferSize );
		builder.withKeyType( keyType );
    	
		AsyncController<?> controller = builder.build();
    	controller.execute();
	}

}
