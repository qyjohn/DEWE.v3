package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;

public class WorkflowScheduler extends Thread
{
	public AmazonKinesisClient client;
	String jobStream, ackStream;
	List<Shard> ackShards = new ArrayList<Shard>();
	Map<String, String> ackIterators = new HashMap<String, String>();

	Workflow workflow;
	String uuid, s3Bucket, s3Prefix;
	int timeout;
	boolean completed;
	
	public WorkflowScheduler(String bucket, String prefix, int t)
	{
		try
		{
			// The Kinesis stream to publish jobs
			Properties prop = new Properties();
			InputStream input = WorkflowScheduler.class.getResourceAsStream("/stream.properties");
			prop.load(input);
			jobStream = prop.getProperty("jobStream");
	
			// Each instance of WorkflowScheduler is a single thread, managing a single workflow.
			// A workflow is represented by a UUID, and the ACK stream is named with the same UUID.
			uuid = UUID.randomUUID().toString();
			ackStream = uuid;
			client = new AmazonKinesisClient();
			createAckStream();	// The Kinesis stream to receive ACK messages 
			listAckShards();
	
			s3Bucket = bucket;
			s3Prefix = prefix;
			timeout = t;
			
			workflow = new Workflow(uuid, s3Bucket, s3Prefix, timeout);
			completed  = false;
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	

	public void createAckStream()
	{
		// Creating the stream
		CreateStreamRequest createStreamRequest = new CreateStreamRequest();
		createStreamRequest.setStreamName(ackStream);
		createStreamRequest.setShardCount(1);
		client.createStream(createStreamRequest);

		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(ackStream);
		long startTime = System.currentTimeMillis();
		long endTime = startTime + ( 10 * 60 * 1000 );
		while ( System.currentTimeMillis() < endTime ) 
		{
			try 
			{
				System.out.println("Waiting for ACK stream " + uuid + " to become active...");
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
			System.out.println("ACK stream " + uuid + " never becomes active. Exiting...");
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

	
	public void deleteAckStream()
	{
		client.deleteStream(uuid);	
	}
	
	public void initialDispatch()
	{
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
			System.out.println("\n\nDispatching " + job.jobId + "\t" + job.jobName+ "\t" + job.ready);
			System.out.println(job.jobXML);

			byte[] bytes = job.jobXML.getBytes();
			PutRecordRequest putRecord = new PutRecordRequest();
			putRecord.setStreamName(jobStream);
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
					System.out.println(job + " is now completed.");
					setJobAsComplete(job);
				}

				ackIterators.put(shardId, getRecordsResult.getNextShardIterator());
			}
		}
	}

	public static void main(String[] args)
	{
		try
		{
			WorkflowScheduler scheduler = new WorkflowScheduler(args[0], args[1], 100);
			scheduler.initialDispatch();
			scheduler.run();
			scheduler.deleteAckStream();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}

