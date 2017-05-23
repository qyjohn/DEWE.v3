package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import net.qyjohn.dewev3.worker.*;
import org.apache.log4j.Logger;

public class LambdaWorkflowScheduler extends Thread
{
	public AmazonKinesisClient client;
	String jobStream, longStream, ackStream;
	List<Shard> ackShards = new ArrayList<Shard>();
	Map<String, String> ackIterators = new HashMap<String, String>();

	LambdaWorkflow workflow;
	String uuid, s3Bucket, s3Prefix;
	boolean localExec, cleanUp, completed;
	
	LambdaLocalWorker worker;
	final static Logger logger = Logger.getLogger(LambdaWorkflowScheduler.class);
	
	public LambdaWorkflowScheduler(String bucket, String prefix)
	{
		try
		{
			// The Kinesis stream to publish jobs
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			jobStream = prop.getProperty("jobStream");
			localExec = Boolean.parseBoolean(prop.getProperty("localExec"));
			cleanUp   = Boolean.parseBoolean(prop.getProperty("cleanUp"));
	
			// Each instance of WorkflowScheduler is a single thread, managing a single workflow.
			// A workflow is represented by a UUID, and the ACK stream is named with the same UUID.
			uuid = "DEWEv3-" + UUID.randomUUID().toString();
			ackStream = uuid;
			longStream = ackStream + "-Long-Jobs";
			client = new AmazonKinesisClient();
			createStream(ackStream);	// The Kinesis stream to receive ACK messages 
			createStream(longStream);	// The Kinesis stream to publish long running jobs 
			listAckShards();
	
			s3Bucket = bucket;
			s3Prefix = prefix;			
			logger.info("Parsing workflow definitions...");
			workflow = new LambdaWorkflow(uuid, s3Bucket, s3Prefix, localExec);
			completed  = false;
			
			// Run one instance of the DeweWorker in the background
			worker = new LambdaLocalWorker(longStream, cleanUp);
			worker.start();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	

	public void createStream(String stream)
	{
		// Creating the stream
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(stream);
		createStreamRequest.setShardCount(1);
		client.createStream(createStreamRequest);

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(stream);
		long startTime = System.currentTimeMillis();
		long endTime = startTime + ( 10 * 60 * 1000 );
		while ( System.currentTimeMillis() < endTime ) 
		{
			try 
			{
				logger.info("Waiting for stream " + stream + " to become active...");
				Thread.sleep(10 * 1000);
				DescribeStreamResult describeStreamResponse = client.describeStream( describeStreamRequest );
				String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
				if ( streamStatus.equals( "ACTIVE" ) ) 
				{
					break;
				}
			} catch (Exception e ) {}
		}
		
		if ( System.currentTimeMillis() >= endTime ) 
		{
			logger.error("Stream " + stream + " never becomes active. Exiting...");
			System.exit(0);
		}
	}

	public void listAckShards()
	{
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(ackStream);
		String exclusiveStartShardId = null;
		do 
		{
			describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
			DescribeStreamResult describeStreamResult = client.describeStream( describeStreamRequest );
			ackShards.addAll( describeStreamResult.getStreamDescription().getShards() );
			if (describeStreamResult.getStreamDescription().getHasMoreShards() && ackShards.size() > 0) 
			{
				exclusiveStartShardId = ackShards.get(ackShards.size() - 1).getShardId();
			} 
			else 
			{
				exclusiveStartShardId = null;
			}
		} while ( exclusiveStartShardId != null );

		for (Shard shard : ackShards)
		{
			String shardId = shard.getShardId();
			String shardIterator;
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(ackStream);
			getShardIteratorRequest.setShardId(shardId);
			getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

			GetShardIteratorResult getShardIteratorResult = client.getShardIterator(getShardIteratorRequest);
			shardIterator = getShardIteratorResult.getShardIterator();			
			ackIterators.put(shardId, shardIterator);
		}
	}

	
	public void deleteStream(String stream)
	{
		client.deleteStream(stream);	
	}
	
	public void initialDispatch()
	{
		logger.info("Begin workflow execution.");
		for (WorkflowJob job : workflow.jobs.values())	
		{
			if (job.ready)
			{
				dispatchJob(job.jobId);
			}
		}	
	}
	
	
	/**
	 *
	 * Publishing a job to the jobStream for the worker node (a Lambda function) to pickup.
	 *
	 */
	 
	public void dispatchJob(String id)
	{
		WorkflowJob job = workflow.jobs.get(id);

		if (job != null)
		{
			logger.info("Dispatching " + job.jobId + ":\t" + job.jobName);

			byte[] bytes = job.jobXML.getBytes();
			PutRecordRequest putRecord = new PutRecordRequest();
			if (job.isLongJob)
			{
				putRecord.setStreamName(longStream);				
			}
			else
			{
				putRecord.setStreamName(jobStream);
			}
			putRecord.setPartitionKey(UUID.randomUUID().toString());
			putRecord.setData(ByteBuffer.wrap(bytes));

			try 
			{
				client.putRecord(putRecord);
			} catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();	
			}
		}		
	}
	
	
	/**
	 *
	 * The worker node (a Lambda function) sends an ACK message to the ackStream, indicating a particular job is now complete.
	 *
	 */
	 
	public void setJobAsComplete(String id)
	{		
		WorkflowJob job = workflow.jobs.get(id);

		if (job != null)
		{
			// Get a list of the children jobs
			for (String child_id : job.childrenJobs) 
			{
				// Get a list of the jobs depending on a particular output file
				WorkflowJob childJob = workflow.jobs.get(child_id);
				// Remove this depending parent job
				childJob.removeParent(id);
				if (childJob.ready)
				{
					dispatchJob(childJob.jobId);
				}
			}
			workflow.jobs.remove(id);
		}	
		
		if (workflow.isEmpty())
		{
			completed = true;
		}	
	}
	
	/**
	 *
	 * The run() method.
	 *
	 */
	 
	public void run()
	{
		while (!completed)
		{
			// Listen for ackStream for completed jobs and update job status
			for (Shard shard : ackShards)
			{
				String shardId = shard.getShardId();
				GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
				getRecordsRequest.setShardIterator(ackIterators.get(shardId));
				getRecordsRequest.setLimit(100);

				GetRecordsResult getRecordsResult = client.getRecords(getRecordsRequest);
				List<Record> records = getRecordsResult.getRecords();
				for (Record record : records)
				{
					String job = new String(record.getData().array());
					logger.info(job + " is now completed.");
					setJobAsComplete(job);
				}

				ackIterators.put(shardId, getRecordsResult.getNextShardIterator());
			}
		}
		logger.info("Workflow is now completed.");

		//delete the ackStream and the longString.
		deleteStream(ackStream);
		deleteStream(longStream);
		System.exit(0);
	}

	public static void main(String[] args)
	{
		try
		{
			LambdaWorkflowScheduler scheduler = new LambdaWorkflowScheduler(args[0], args[1]);
			scheduler.initialDispatch();
			scheduler.run();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}

