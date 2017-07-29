package actors;

import static utility.Constants.CLIENT_STATUS;
import static utility.Constants.FINISHED;
import static utility.Constants.QUEUELENGTH;
import static utility.Constants.REQUESTQ;
import static utility.Constants.RESPONSEQ;
import static utility.Constants.STARTED;
import static utility.Constants.THROUGHPUT_STATUS;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import monitor.ClientMonior;
import monitor.WorkerMonitor;
import monitor.cassandra.SimpleClient;
import queue.DistributedQueue;
import queue.QueueFactory;
import queue.TaskQueueFactory;
import queue.hazelcast.QueueHazelcastUtil;
import utility.PrintManager;

import com.datastax.driver.core.utils.UUIDs;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.util.ConcurrentHashSet;

import entity.QueueDetails;
import entity.Task;
import entity.TemplateTask;

public class StagedClient implements Runnable {

	Map<String, Task> submittedTasks;
	String url;
	QueueDetails qu;
	long pollTime = 3000;
	long throughputpolltime = 1000;
	HazelcastClient hazelClinetObj;
	long numberofWorkerThreads = 1;
	String fileName;
	SimpleClient cassandraClient;
	ClientMonior objClientMonior;
	String clientName;
	String mqPortnumber;
	String taskType;
	String taskCount;
	String tastLength;
	String fileSize;
	String filePath;
	Semaphore stageLock;
	boolean monitoringEnabled = true;
	ConcurrentMap<String, String> mapClientStatus;
	ConcurrentMap<String, String> mapThroughPutStatus;
	ConcurrentMap<String, String> mapQLengthStatus;
	QueueHazelcastUtil objQueueHazelcastUtil;

	public StagedClient(Semaphore stageLock) {
		super();
		try (FileReader reader = new FileReader("CloudKon.properties")) {
			clientName = genUniQID();
			submittedTasks = new ConcurrentHashMap<>();
			Properties properties = new Properties();
			properties.load(reader);
			// hazelClient
			hazelClinetObj = new QueueHazelcastUtil().getClient();
			mapClientStatus = hazelClinetObj.getMap(CLIENT_STATUS);
			mapQLengthStatus = hazelClinetObj.getMap(QUEUELENGTH);
			mapThroughPutStatus = hazelClinetObj.getMap(THROUGHPUT_STATUS);

			numberofWorkerThreads = Long.parseLong(properties
					.getProperty("numberofWorkerThreads"));
			pollTime = Long.parseLong(properties.getProperty("clientPollTime"));
			fileName = properties.getProperty("taskFilePath");
			mqPortnumber = properties.getProperty("mqPortnumber");
			taskType = properties.getProperty("taskType");
			taskCount = properties.getProperty("taskCount");
			tastLength = properties.getProperty("tastLength");
			fileSize = properties.getProperty("fileSize");
			filePath = properties.getProperty("filePath");
			monitoringEnabled = properties.getProperty("monitoringEnabled")
					.equals("true");
			throughputpolltime = Long.parseLong(properties
					.getProperty("monPolltime"));
			this.stageLock = stageLock;
			if (monitoringEnabled) {
				String cassServerlist = properties
						.getProperty("cassServerlist");
				cassandraClient = new SimpleClient();
				cassandraClient.connect(cassServerlist);
				// Create monitor
				objClientMonior = new ClientMonior(clientName, cassandraClient,
						submittedTasks, mapQLengthStatus, hazelClinetObj,
						throughputpolltime);
			}

		} catch (IOException e) {
			PrintManager.PrintException(e);
		}
	}

	private void postTasks(String strFileName) throws NumberFormatException,
			FileNotFoundException, InterruptedException {
		stageLock.acquire();
		Thread.currentThread().setPriority(8);
		Semaphore objSemaphore = new Semaphore(0);

		Set<Task> objects = null;
		url = getUrl();
		qu = new QueueDetails(REQUESTQ, RESPONSEQ, clientName, url);
		PrintManager.PrintMessage("creating tasks");
		objects = readFileAndMakeTasks(strFileName, clientName);
		PrintManager.PrintProdMessage("Posting tasks started "
				+ System.nanoTime());
		TaskQueueFactory.getQueue().postTask(objSemaphore, objects, REQUESTQ,
				url, clientName);

		String time = String.valueOf(System.nanoTime());
		// check the mode of operation

		// Get the already running workers
		long numOfWorkers = WorkerMonitor.getNumOfWorkerThreads(hazelClinetObj);
		PrintManager.PrintProdMessage("numOfWorkers " + numOfWorkers);
		if (numOfWorkers == 0) {
			numOfWorkers = 1;
		}
		long loopCount = objects.size()
				/ (numberofWorkerThreads);
		loopCount = loopCount == 0 ? 1 : loopCount;
		PrintManager.PrintProdMessage("Number of Cleint Q Advertizements "
				+ loopCount);
		DistributedQueue queue = QueueFactory.getQueue();
		objSemaphore.acquire();
		for (int loopIndex = 0; loopIndex < loopCount; loopIndex++) {
			queue.pushToQueue(qu);
		}
		PrintManager.PrintProdMessage("Cleint Q Advertizements done");
		mapClientStatus.putIfAbsent(this.clientName + "," + STARTED, time);
		// Start monitoring the Submitted Queue length for completion
		new Thread(this).start();
		// Start monitoring the Submitted Queue length for reporting
		if (monitoringEnabled) {
			PrintManager
					.PrintProdMessage("monitoringEnabled starting Clinet monitor");
			new Thread(objClientMonior).start();
		}
	}

	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		long startMeasureTime = System.currentTimeMillis();
		long currtime = startMeasureTime;
		int counter = 0;
		while (!submittedTasks.isEmpty()) {
			currtime = System.currentTimeMillis();
			if (currtime - startMeasureTime >= throughputpolltime) {
				startMeasureTime = currtime;
				PrintManager.PrintProdMessage("Recording throughput "
						+ System.nanoTime() + " " + counter);
				mapThroughPutStatus.putIfAbsent(clientName+","+System.nanoTime(),
						String.valueOf(counter));
				counter = 0;
			}
			Task task = TaskQueueFactory.getQueue().retrieveTask(RESPONSEQ,
					url, clientName);
			if (task != null) {
				if (task.isMultiTask()) {
					for (Task tasks : task.getTasks()) {
						PrintManager.PrintMessage("Task[" + tasks.getTaskId()
								+ "]completed");
						submittedTasks.remove(tasks.getTaskId());
						counter++;
					}
				} else {
					PrintManager.PrintMessage("Task[" + task.getTaskId()
							+ "]completed");
					submittedTasks.remove(task.getTaskId());
					counter++;
				}

			}
			// Advertise tasks again after some time
			if (System.currentTimeMillis() - startTime > pollTime) {
				PrintManager.PrintProdMessage("PANIC !!! Queue Resubmistions by Client at " +System.nanoTime());
				startTime = System.currentTimeMillis();
				DistributedQueue queue = QueueFactory.getQueue();
				queue.pushToQueue(qu);
			}

		}
		if (monitoringEnabled) {
			// Shutdown monitor
			objClientMonior.setClientShutoff(true);
		}
		String time = String.valueOf(System.nanoTime());
		String[] valFin = { clientName, String.valueOf(System.nanoTime()),
				FINISHED };
		mapClientStatus.putIfAbsent(this.clientName + "," + FINISHED, time);
		if (monitoringEnabled) {
			cassandraClient.insertClientStatus(valFin);
			try {
				Thread.sleep(6000);
			} catch (InterruptedException e) {
				PrintManager.PrintException(e);
			}
		}

		PrintManager.PrintProdMessage("Shutting down Client " + this.clientName
				+ " at " + time);
		// Shutdown hazel
		hazelClinetObj.shutdown();
		if (monitoringEnabled) {
			cassandraClient.close();
		}
		PrintManager.PrintProdMessage("Lock status"
				+ this.stageLock.hasQueuedThreads());
		this.stageLock.release();
	}

	private Set<Task> readFileAndMakeTasks(String fileName, String clientName)
			throws NumberFormatException, FileNotFoundException {
		Scanner s = new Scanner(new File(fileName));
		Set<Task> list = new ConcurrentHashSet<Task>();
		Task task = null;
		while (s.hasNext()) {
			task = new TemplateTask(genUniQID() + clientName, clientName,
					RESPONSEQ, url, Long.parseLong(s.next()));
			list.add(task);
			submittedTasks.put(task.getTaskId(), task);
		}
		s.close();
		return list;
	}

	private String genUniQID() {
		UUID uniqueID = UUIDs.timeBased();
		return uniqueID.toString();
	}

	private String getUrl() {
		InetAddress addr;
		String ipAddress = null;
		try {
			addr = InetAddress.getLocalHost();
			ipAddress = addr.getHostAddress();
		} catch (UnknownHostException e) {
			PrintManager.PrintException(e);
		}
		return "tcp://" + ipAddress + ":" + mqPortnumber;
	}

	public static void main(Semaphore stageLock, String strFileName)
			throws Exception {
		StagedClient client = new StagedClient(stageLock);
		client.postTasks(strFileName);

	}
}
