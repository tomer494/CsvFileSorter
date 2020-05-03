package com.ob1tech.AsyncCsvFileSorter.controllers;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.ob1tech.CsvFileSorter.controllers.ControllerBuilder;

public class AsyncControllerBuilder extends ControllerBuilder{
	
	public AsyncControllerBuilder(String filePath) {
		super(filePath);
		// TODO Auto-generated constructor stub
	}

	public AsyncController<?> build() {		
		AsyncController<?> controller;
		switch(getKeyType()) {
		case "string": controller = new AsyncController<String>(); break;
		case "double": controller = new AsyncController<Double>(); break;
		default: controller = new AsyncController<Long>(); break;
		}
		Path path = Paths.get(getFilePath());
		controller.setFilePath(path);
		controller.setBatchSize(getBatchSize());
		controller.setKeyIndex(getKeyIndex());
		controller.setSkipHeader(isSkipHeader());
		controller.setKeyType(getKeyType());
		return controller;
	}
	
	
}
