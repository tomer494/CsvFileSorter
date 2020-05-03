package com.ob1tech.CsvFileSorter.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * This class aims to centralize inner program utilities for extended use.
 * It contains a ThreadPool and file utils
 * @author Madmon Tomer
 *
 */
public class Utilities {
	
	private static final int COREPOOLSIZE = 10;
	
	private static Logger logger = LogManager.getLogger(Utilities.class);
	private ThreadPoolExecutor threadPool;
	private static Object saveLock = new Object();
	
	public Utilities() {
		super();
		ThreadFactory threadFactory = new ThreadFactory() {
			   private final AtomicLong threadIndex = new AtomicLong(0);

			   @Override
			   public Thread newThread(Runnable runnable) {
			       Thread thread = new Thread(runnable);
			       thread.setName("fileUtilities-" + threadIndex.getAndIncrement());
			       return thread;
			   }
			};
		threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(COREPOOLSIZE);
		threadPool.setThreadFactory(threadFactory);
	}

	public Utilities(int corePoolSize, String threadName) {
		super();
		ThreadFactory threadFactory = new ThreadFactory() {
			   private final AtomicLong threadIndex = new AtomicLong(0);

			   @Override
			   public Thread newThread(Runnable runnable) {
			       Thread thread = new Thread(runnable);
			       thread.setName(threadName + "-" + threadIndex.getAndIncrement());
			       return thread;
			   }
			};
		threadPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(corePoolSize);
		threadPool.setThreadFactory(threadFactory);
	}

	public static String constractFileName(Path basedOnFileName, long nodeIndex, String type) {
		return basedOnFileName.getFileName().toString().concat(type+nodeIndex);
	}

	public static Path constractFilePath(Path basedOnFileName, long nodeIndex, String type) {
		String constractedFileName = constractFileName(basedOnFileName, nodeIndex, type);
		return resolve(basedOnFileName.getParent(), constractedFileName);
	}

	public static Path resolve(Path directory, String fileName) {
		return directory.resolve(fileName);
	}

	public static Object getValueOf(Path filePath, ObjectMapper mapper, Class<?> clazz) {
		Object value = null;
		synchronized (saveLock) {
			try {
				//logger.info("Reading "+filePath.getFileName().toString());
				List<String> readLines = Files.readAllLines(filePath);
				if(!readLines.isEmpty()) {
					String rawString = readLines.get(0);
					value = mapper.readValue(rawString, clazz);
				}else {
					logger.error("Error reading file "+filePath.getFileName().getFileName());
				}
			} catch (FileNotFoundException e) {
				logger.error("Error reading "+filePath.getFileName(),e);
			} catch (IOException e) {
				logger.error("Error reading "+filePath.getFileName(),e);
			} catch (Exception e) {
				logger.error("Error reading "+filePath.getFileName(),e);
			}finally {
				
			}
		}
		return value;
	}
	
	public static void save(Object obj, Path filePath, ObjectMapper mapper) {
		File file;
		FileOutputStream fileOut = null;
		synchronized (saveLock) {
			try {
					file = filePath.toFile();
					fileOut = new FileOutputStream(file);
					String serialized = mapper.writeValueAsString(obj);
					fileOut.write(serialized.getBytes());
			
			} catch (Exception ex) {
				logger.error("Error writing "+filePath.getFileName(),ex);
			}finally {
				try {
					if(fileOut!=null) {
						fileOut.flush();
						fileOut.close();
					}
				} catch (IOException e) {
					logger.error("Error closing "+filePath.getFileName(),e);
				}
			}
		}
	}

	public static String readLine(Path path) {
		String line = null;
		try {
			try (BufferedReader br = new BufferedReader(new FileReader(path.toFile()))) {
				line = br.readLine();
			}
		} catch (FileNotFoundException e) {
			logger.error("Error reading "+path.getFileName(),e);
		} catch (IOException e) {
			logger.error("Error reading "+path.getFileName(),e);
		}
		return line;
	}
	
	public static Path moveFile(Path fromFilePath, String toFileName) {
		return moveFile(fromFilePath, toFileName, false);
	}

	public static Path moveFile(Path fromFilePath, String toFileName, boolean deleteTargetIfExists) {
		Path targetPath = fromFilePath.getParent();
		Path resolvedTarget = targetPath.resolve(toFileName);
		return moveFile(fromFilePath, resolvedTarget, deleteTargetIfExists);
	}
	public static Path moveFile(Path fromFilePath, Path toFilePath, boolean deleteTargetIfExists) {
			
		Path targetFile = null;
		
		try {
			if(deleteTargetIfExists) {
				Files.deleteIfExists(toFilePath);
			}
			targetFile = Files.move(fromFilePath, toFilePath);
		} catch (IOException e) {
			logger.error("Error moving from "+fromFilePath.getFileName()+
					" to "+toFilePath.getFileName(),e);
		}
		return targetFile;
	}

	
	

	public ThreadPoolExecutor getThreadPool() {
		return threadPool;
	}
	
}
