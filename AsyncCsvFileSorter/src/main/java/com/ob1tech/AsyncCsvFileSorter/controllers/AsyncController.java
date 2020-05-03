package com.ob1tech.AsyncCsvFileSorter.controllers;


import static com.ob1tech.CsvFileSorter.controllers.BatchController.BATCH_SUFFIX;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.ob1tech.CsvFileSorter.controllers.BatchController;
import com.ob1tech.CsvFileSorter.controllers.Controller;
import com.ob1tech.CsvFileSorter.dateModel.RecordBatchNode;
import com.ob1tech.CsvFileSorter.utils.Utilities;

/**
 * Asyncronius extention of {@link Controller}
 * Implements a task queue to handle batches. {@link RecordBatchQueue}
 * Implements an Asyncronius {@link BatchController}
 * @author Madmon Tomer
 *
 * @param <T>
 * @see Controller
 */
public class AsyncController<T extends Comparable<T>> extends Controller<T> implements Observer {
	
	private static final String QUEUE_SUFFIX = ".que";

	private static final int COREPOOLSIZE = 20;
	
	private AtomicLong tasksCount;
	private Object tasklock;
	private Object runlock;
	private Object lock;
	private boolean ocupied;
	private boolean endReadingTheFile;
	boolean finished;
	private RecordBatchQueue recordBatchQueue = null;
	private Utilities threadPoolUtilities;


	public AsyncController() {
		super();
		tasksCount = new AtomicLong();
		lock = new Object();
		tasklock = new Object();
		runlock = new Object();
		endReadingTheFile = false;
		finished = true;
		setRecordToBatchMap(new ConcurrentHashMap<Long, Long>());
		
	}


	@Override
	public void initBatchController(Path filePath, int batchSize, String keyDataType,
			Map<Long, Long> recordToBatchMap) {
		setBatchController(new AsyncBatchController<T>(filePath, batchSize, keyDataType, recordToBatchMap));
	}



	@Override
	public void execute() {
		threadPoolUtilities = new Utilities(COREPOOLSIZE, "BatchControllerUpdateor");
		super.execute();
		endReadingTheFile = true;
		
		synchronized (runlock) {
			boolean queueIsEmpty = recordBatchQueue.waitingBatches.size()==0;
			
			while(!queueIsEmpty || recordBatchQueue.runningTreads.get()>0) {
				//wait a bit before checking if another task is waiting
				try {
					runlock.wait(60*1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				queueIsEmpty = recordBatchQueue.waitingBatches.size()==0;
			}
		}
		super.finalizeBatchController();
		super.writeSortedFile();
		
	}
	
	@Override
	public void finalizeBatchController() {
		// Wait for queue
		
	}
	
	@Override
	public void writeSortedFile() {
		// Wait for queue		
	}

	/**
	 * Overide the origionat to set up queue and pusshing nodes to it
	 */
	@Override
	protected void updateBatchController(RecordBatchNode<T> recordBatchNode) {
		
		
		if(recordBatchQueue == null) {
			recordBatchQueue = new RecordBatchQueue(getBatchController());
			((Observable)recordBatchQueue).addObserver(this);
			Thread queueThread = new Thread( recordBatchQueue );
			queueThread.start();
			
			tasksCount.getAndIncrement();
			Runnable BatchControllerUpdateor = new BatchControllerUpdateor(recordBatchNode);
			((Observable)BatchControllerUpdateor).addObserver(recordBatchQueue);
			recordBatchQueue.runningTreads.getAndIncrement();
			
			threadPoolUtilities.getThreadPool().execute(BatchControllerUpdateor);
			
		} else {
		
			//TODO TEST MODE
			boolean testMode = false;
			if(testMode) {
				synchronized (tasklock) {
					try {
						tasklock.wait(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			putToQueue(recordBatchNode);
		
		}
		
		
		
	}
	
	private void superUpdateBatchController(RecordBatchNode<T> recordBatchNode) {			
		super.updateBatchController(recordBatchNode);
	}

	/**
	 * Adds RecordBatchNode to qeuqeu for later proccess
	 * @param recordBatchNode
	 */
	private void putToQueue(RecordBatchNode<T> recordBatchNode) {
		long batchId = tasksCount.getAndIncrement();
		Path filePath = Utilities.constractFilePath(getFilePath(), batchId, QUEUE_SUFFIX);
		Utilities.save(recordBatchNode, filePath , getBatchController().getObjectMapper());
		recordBatchQueue.waitingBatches.add(batchId);
	}
	
	private class BatchControllerUpdateor extends Observable implements Runnable{
		
		private RecordBatchNode<T> recordBatchNode;

		public BatchControllerUpdateor(RecordBatchNode<T> recordBatchNode) {
			super();
			this.recordBatchNode = recordBatchNode;
		}

		public void run() {
			superUpdateBatchController(recordBatchNode);
			//Let the queue know a task has finished
			setChanged();
			notifyObservers();
		}

		public RecordBatchNode<T> getRecordBatchNode() {
			return recordBatchNode;
		}
		
	}
	
	/**
	 * Batch queue to controll adding batch node elements to the sorting tree.
	 * @author Madmon Tomer
	 *
	 */
	private class RecordBatchQueue extends Observable implements Runnable, Observer{
		
		private BatchController<T> batchController;
		private Object queueLock;
		private List<Long> waitingBatches;
		private AtomicInteger runningTreads;
		

		public RecordBatchQueue(BatchController<T> batchController) {
			super();
			this.batchController = batchController;
			queueLock = new Object();
			waitingBatches = new LinkedList<Long>();
			runningTreads = new AtomicInteger();
		}

		public void run() {
			//finished = false;
			long currentTaskCount = tasksCount.get();
			//runningTreads.getAndSet((int) tasksCount.get());
			boolean queueIsEmpty = waitingBatches.isEmpty();
			
			while (!(endReadingTheFile && waitingBatches.size()==0)) {
				String message = String.format("QUEUE: Reading File is %s, Queue is %s empty, Waiting %s, Handled indexes: %s",
						(endReadingTheFile?"done!":"in progress..."),
						(queueIsEmpty?"":"not"),
						currentTaskCount-batchController.getBatchCounter()-1,
						waitingBatches.size());
				logger.info(message);
				boolean waitABit = true;
				if (!queueIsEmpty && batchController.getBatchCounter()>0) {
					boolean isOcupied = false;


					if (runningTreads.get()<maxConcurrentAllowed()) {
						runningTreads.getAndIncrement();
						//!isOcupied) {
						
						long batchId = waitingBatches.remove(0);
						logger.info("QUEUE: Handle queued task " + batchId);
						Path queueFilePath = Utilities.constractFilePath(getFilePath(), batchId, QUEUE_SUFFIX);
						Path batchFilePath = Utilities.constractFilePath(getFilePath(), batchId, BATCH_SUFFIX);
						Utilities.moveFile(queueFilePath, batchFilePath, true);
						@SuppressWarnings("unchecked")
						RecordBatchNode<T> recordBatchNode = (RecordBatchNode<T>) Utilities.getValueOf(batchFilePath,
								batchController.getObjectMapper(), RecordBatchNode.class);
						
						Runnable BatchControllerUpdateor = new BatchControllerUpdateor(recordBatchNode);
						((Observable)BatchControllerUpdateor).addObserver(this);
						threadPoolUtilities.getThreadPool().execute(BatchControllerUpdateor);
						
						
						//Task time is sufficient waiting time
						waitABit = false;
					}
				}
				if (waitABit) {
					synchronized (queueLock) {
						//wait a bit before checking if another task is waiting
						try {
							queueLock.wait(60*1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
				currentTaskCount = tasksCount.get();
				queueIsEmpty = waitingBatches.isEmpty();
				
			}
			while(runningTreads.get()>0) {
				synchronized (queueLock) {
					//wait a bit before checking if another task is waiting
					try {
						queueLock.wait(100);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			//Let the controller know the queue is done with all the tasks
			setChanged();
			notifyObservers();
			
		}

		public int maxConcurrentAllowed() {
			boolean isDebug = java.lang.management.ManagementFactory.getRuntimeMXBean()
					.getInputArguments().toString().contains("-agentlib:jdwp");
			return isDebug?getBatchSize()/1:getBatchSize();
		}

		@SuppressWarnings("unchecked")
		@Override
		public void update(Observable o, Object arg) {
			runningTreads.decrementAndGet();
			Long id = ((BatchControllerUpdateor)o).getRecordBatchNode().getId();
			logger.info("Queue Notified on finished batch "+id
					+" running "+runningTreads);
			
			synchronized (queueLock) {
				queueLock.notify();
			}
			
		}
		
	}

	@Override
	public void update(Observable o, Object arg) {
		logger.info("Controler Notified!");
		synchronized (runlock) {
			runlock.notify();
		}
	}

}
