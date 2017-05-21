package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import net.qyjohn.dewev3.worker.*;
import org.apache.log4j.Logger;


public class WorkflowSchedulerGoogle extends Thread
{
	GoogleWorkflow workflow;
	GoogleTransceiver transceiver;
	String uuid, gsBucket, gsPrefix;
	boolean localExec, cleanUp, completed;
	
	LocalWorkerGoogle worker;
	final static Logger logger = Logger.getLogger(WorkflowSchedulerGoogle.class);
	
	public WorkflowSchedulerGoogle(String bucket, String prefix)
	{
		try
		{
			// The runtime properties
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			String jobTopic  = prop.getProperty("jobTopic");
			localExec = Boolean.parseBoolean(prop.getProperty("localExec"));
			cleanUp   = Boolean.parseBoolean(prop.getProperty("cleanUp"));

			// Each instance of WorkflowScheduler is a single thread, managing a single workflow.
			// A workflow is represented by a UUID, and the ACK stream is named with the same UUID.
			uuid = "DEWEv3-" + UUID.randomUUID().toString();
			transceiver = new GoogleTransceiver(uuid, jobTopic);

			gsBucket = bucket;
			gsPrefix = prefix;			
			logger.info("Parsing workflow definitions...");
			workflow = new GoogleWorkflow(uuid, gsBucket, gsPrefix, localExec);
			completed  = false;
			
			// Run one instance of the DeweWorker in the background
//			worker = new LocalWorkerGoogle(uuid, cleanUp);
//			worker.start();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	
	
	public void initialDispatch()
	{
		logger.info("Begin workflow execution.");
		for (WorkflowJob job : workflow.jobs.values())	
		{
			if (job.ready)
			{
				transceiver.publishJob(job);
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
					transceiver.publishJob(childJob);
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
			try
			{
				String ack = transceiver.receiveAck();
				if (ack != null)
				{
					logger.info(ack + " is now completed.");
					setJobAsComplete(ack);
				}
				else
				{
					sleep(100);
				}
			} catch (Exception e)
			{
			}
		}
		logger.info("Workflow is now completed.");

		//delete the ackStream and the longString.
		transceiver.cleanUp();
		System.exit(0);
	}

	public static void main(String[] args)
	{
		try
		{
			WorkflowSchedulerGoogle scheduler = new WorkflowSchedulerGoogle(args[0], args[1]);
			scheduler.initialDispatch();
			scheduler.run();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

}

