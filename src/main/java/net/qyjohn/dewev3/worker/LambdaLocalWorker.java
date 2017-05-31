package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.*; 
import com.amazonaws.services.lambda.runtime.events.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

public class LambdaLocalWorker extends Thread
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonSQSClient sqsClient = new AmazonSQSClient();
	public AmazonKinesisClient kinesisClient;
	public String tempDir = "/tmp";
	public String longQueue, ackQueue;
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	volatile boolean completed = false;
	Stack<String> jobStack = new Stack<String>();
	Stack<String> uploadStack = new Stack<String>();
	Stack<String> downloadStack = new Stack<String>();
	// Logging
	final static Logger logger = Logger.getLogger(LambdaLocalWorker.class);

	
	/**
	 *
	 * Constructor for worker node handling long running jobs. 
	 * In this case, the long running job stream is needed.
	 *
	 */

	public LambdaLocalWorker(String tempDir)
	{
		try
		{
			this.tempDir = tempDir;

			// The Kinesis stream to publish jobs
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			longQueue = prop.getProperty("longQueue");
			ackQueue  = prop.getProperty("ackQueue");

			s3Client = new AmazonS3Client();
			kinesisClient = new AmazonKinesisClient();

			cachedFiles = new ConcurrentHashMap<String, Boolean>();
			int nProc = Runtime.getRuntime().availableProcessors();
			LambdaLocalExecutor executors[] = new LambdaLocalExecutor[nProc];
			for (int i=0; i<nProc; i++)
			{
				executors[i] = new LambdaLocalExecutor(ackQueue, tempDir, cachedFiles);
				executors[i].setJobStack(jobStack);
				executors[i].start();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
		
	/**
	 *
	 * When the job handler runs on an EC2 instance, it is a long running thread.
	 *
	 */
	 
	public void run()
	{
		while (!completed)
		{
			try
			{
				ReceiveMessageResult result = sqsClient.receiveMessage(longQueue);
				for (Message message : result.getMessages())
				{
					String jobXML = message.getBody();
					logger.debug(jobXML);
					jobStack.push(jobXML);
					sqsClient.deleteMessage(longQueue, message.getReceiptHandle());
				}				
			} catch (Exception e)
			{
			}
		}
	}
	
	
	/**
	 *
	 * Mark the workflow as completed. This is used for the EC2 job handler to exit gracefully.
	 *
	 */
	 
	public void setAsCompleted()
	{
		completed = true;
	}
	
	public static void main(String[] args)
	{
		try
		{
			// Create a temp folder for the execution environment
			String tempDir = "/tmp/" + UUID.randomUUID().toString();
			Process p = Runtime.getRuntime().exec("mkdir -p " + tempDir);
			p.waitFor();

			// Start the LambdaLocalWorker
			LambdaLocalWorker worker = new LambdaLocalWorker(tempDir);
			worker.start();
			worker.join();			
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}
