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

public class LocalWorker extends Thread
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	// For long running jobs
	volatile boolean completed = false, cleanUp = false;
	String longStream;
	List<Shard> longShards = new ArrayList<Shard>();
	Map<String, String> longIterators = new HashMap<String, String>();
	Stack<String> jobStack = new Stack<String>();
	// Logging
	final static Logger logger = Logger.getLogger(LocalWorker.class);

	
	/**
	 *
	 * Constructor for worker node handling long running jobs. 
	 * In this case, the long running job stream is needed.
	 *
	 */

	public LocalWorker(String longStream, boolean cleanUp)
	{
		try
		{
			this.longStream = longStream;
			this.cleanUp = cleanUp;
			tempDir = "/tmp/" + longStream;
			Process p = Runtime.getRuntime().exec("mkdir -p " + tempDir);
			p.waitFor();

			ClientConfiguration clientConfig = new ClientConfiguration();
			clientConfig.setMaxConnections(100);
			s3Client = new AmazonS3Client(clientConfig);
			kinesisClient = new AmazonKinesisClient();
			listLongShards();

			cachedFiles = new ConcurrentHashMap<String, Boolean>();
			int nProc = Runtime.getRuntime().availableProcessors();
			DeweExecutor executors[] = new DeweExecutor[nProc];
			for (int i=0; i<nProc; i++)
			{
				executors[i] = new DeweExecutor(s3Client, kinesisClient, tempDir, cachedFiles);
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
	 * The long running job handler receives jobs from a separate Kinesis stream.
	 *
	 */

	public void listLongShards()
	{
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(longStream);
		String exclusiveStartShardId = null;
		do 
		{
			describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
			DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
			longShards.addAll( describeStreamResult.getStreamDescription().getShards() );
			if (describeStreamResult.getStreamDescription().getHasMoreShards() && longShards.size() > 0) 
			{
				exclusiveStartShardId = longShards.get(longShards.size() - 1).getShardId();
			} 
			else 
			{
				exclusiveStartShardId = null;
			}
		} while ( exclusiveStartShardId != null );

		for (Shard shard : longShards)
		{
			String shardId = shard.getShardId();
			String shardIterator;
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(longStream);
			getShardIteratorRequest.setShardId(shardId);
			getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

			GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
			shardIterator = getShardIteratorResult.getShardIterator();			
			longIterators.put(shardId, shardIterator);
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
				// Listen for longStream for jobs to execute
				for (Shard shard : longShards)
				{
					String shardId = shard.getShardId();
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					getRecordsRequest.setShardIterator(longIterators.get(shardId));
					getRecordsRequest.setLimit(100);
	
					GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
					List<Record> records = getRecordsResult.getRecords();
					for (Record record : records)
					{
						String jobXML = new String(record.getData().array());
						jobStack.push(jobXML);
					}
	
					longIterators.put(shardId, getRecordsResult.getNextShardIterator());
				}
			} catch (ResourceNotFoundException e)
			{
				// The longStream has been deleted. The workflow has completed execution
				completed = true;
			}
		}

		// Remove temp folder
		if (cleanUp)
		{
			try
			{
				Process p = Runtime.getRuntime().exec("rm -Rf " + tempDir);
				p.waitFor();
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
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
}
