package com.ob1tech.CsvFileSorter.controllers;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ControllerBuilder {
	
	private String filePath;
	
	private int batchSize = Controller.DEFAULT_BUFFER_SIZE;	
	private int keyIndex = 0;
	private boolean skipHeader = false;
	private String keyType = "";

	public ControllerBuilder withBatchSize(int batchSize) {
		if(batchSize>0) {
			this.batchSize = batchSize;
		}
		return this;
	}

	public ControllerBuilder withKeyIndex(int keyIndex) {
		this.keyIndex = keyIndex;
		return this;
	}

	public ControllerBuilder withSkipHeader(boolean skipHeader) {
		this.skipHeader = skipHeader;
		return this;
		
	}

	public ControllerBuilder withKeyType(String keyType) {
		if(keyType!=null) {
			this.keyType = keyType;
		}
		return this;
	}

	public ControllerBuilder(String filePath) {
		this.filePath = filePath;
	}
	
	public Controller<?> build() {		
		Controller<?> controller;
		String keyDataType;
		switch(keyType) {
		case "string": keyDataType = String.class.getTypeName(); controller = new Controller<String>(); break;
		case "double": keyDataType = Double.class.getTypeName(); controller = new Controller<Double>(); break;
		default: keyDataType = Long.class.getTypeName(); controller = new Controller<Long>(); break;
		}
		Path path = Paths.get(filePath);
		controller.setFilePath(path);
		controller.setBatchSize(batchSize);
		controller.setKeyIndex(keyIndex);
		controller.setSkipHeader(skipHeader);
		//controller.setKeyType(keyType);
		controller.setKeyDataType(keyDataType);
		return controller;
	}
	
	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
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

	
}
