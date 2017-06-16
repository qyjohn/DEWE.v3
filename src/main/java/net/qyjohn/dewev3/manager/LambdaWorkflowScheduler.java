package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;
import net.qyjohn.dewev3.worker.*;
import org.apache.log4j.Logger;

public class LambdaWorkflowScheduler extends Thread
{
	public AmazonKinesisClient kinesisClient = new AmazonKinesisClient();
	public AmazonSQSClient sqsClient = new AmazonSQSClient();
	String jobStream, ackStream;
	String longQueue, ackQueue, deweComm;
	List<Shard> ackShards = new ArrayList<Shard>();
	Map<String, String> ackIterators = new HashMap<String, String>();

	LambdaWorkflow workflow;
	String uuid, s3Bucket, s3Prefix, tempDir;
	boolean localExec, cleanUp, completed;
	public String caching = "false";
	public int localPerc=0;
	
	LambdaLocalWorkerV2 worker;
	final static Logger logger = Logger.getLogger(LambdaWorkflowScheduler.class);
	
	Date d1, d2;
	
	public LambdaWorkflowScheduler(String bucket, String prefix)
	{
		try
		{
			// System Properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			jobStream = prop.getProperty("jobStream");
			longQueue = prop.getProperty("longQueue");
			ackQueue = prop.getProperty("ackQueue");
			localExec = Boolean.parseBoolean(prop.getProperty("localExec"));
			cleanUp   = Boolean.parseBoolean(prop.getProperty("cleanUp"));
			localPerc = Integer.parseInt(prop.getProperty("localPerc"));
	
			// House keeping
			purgeQueue();

			// Parsing workflow definitions
			logger.info("Parsing workflow definitions...");
			uuid = UUID.randomUUID().toString();
			workflow = new LambdaWorkflow(uuid, bucket, prefix, localExec, ackQueue);
			completed  = false;
			
			// Run one instance of the DeweWorker in the background
			tempDir = "/tmp/" + UUID.randomUUID().toString();
			Process p = Runtime.getRuntime().exec("mkdir -p " + tempDir);
			p.waitFor();
			worker = new LambdaLocalWorkerV2(tempDir);
			worker.start();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	
	
	public void initialDispatch()
	{
		d1 = new Date();
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
			boolean success = false;
			while (!success)
			{
				try
				{
					if (job.isLongJob)
					{
						sqsClient.sendMessage(longQueue, job.jobXML);				
					}
					else
					{
						int rand = new Random().nextInt(100);
						// Send localPerc% of jobs to the long queue for local execution
						if (rand < localPerc) 
						{
							sqsClient.sendMessage(longQueue, job.jobXML);
						}
						else
						{
							byte[] bytes = job.jobXML.getBytes();
							PutRecordRequest putRecord = new PutRecordRequest();
							putRecord.setStreamName(jobStream);
							putRecord.setPartitionKey(UUID.randomUUID().toString());
							putRecord.setData(ByteBuffer.wrap(bytes));
							kinesisClient.putRecord(putRecord);
						}
					}
					success = true;
				} catch (Exception e) 
				{
					System.out.println(e.getMessage());
					e.printStackTrace();	
				}				
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
			// Pulling the ackQueue
			try
			{
				ReceiveMessageResult result = sqsClient.receiveMessage(ackQueue);
				for (Message message : result.getMessages())
				{
					String job = message.getBody();
					logger.info(job + " is now completed.");
					setJobAsComplete(job);
					sqsClient.deleteMessage(ackQueue, message.getReceiptHandle());
				}				
			} catch (Exception e)
			{
			}
		}
		logger.info("Workflow is now completed.");

		d2 = new Date();
		long seconds = (d2.getTime()-d1.getTime())/1000;
		System.out.println("\n\nTotal execution time: " + seconds + " seconds.\n\n");

		// delete the temp foler
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
		System.exit(0);
	}
	
	
	class AckPuller extends Thread
	{
		public void run()
		{
			AmazonSQSClient c = new AmazonSQSClient();

			while (!completed)
			{
				// Pulling the ackQueue
				try
				{
					ReceiveMessageResult result = c.receiveMessage(ackQueue);
					for (Message message : result.getMessages())
					{
						String job = message.getBody();
						logger.info(job + " is now completed.");
						setJobAsComplete(job);
						c.deleteMessage(ackQueue, message.getReceiptHandle());
					}				
				} catch (Exception e)
				{
				}				
			}	
		}		
	}
	public void purgeQueue()
	{
			try
			{
				PurgeQueueRequest req1 = new PurgeQueueRequest(longQueue);
				sqsClient.purgeQueue(req1);
				PurgeQueueRequest req2 = new PurgeQueueRequest(ackQueue);
				sqsClient.purgeQueue(req2);
			} catch (Exception e)
			{
			}
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

