package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.pubsub.v1.*;

public class LocalWorkerGoogle extends Thread
{
	// Common components
	public String uuid;
	public String projectId = ServiceOptions.getDefaultProjectId();
	public SubscriptionAdminClient subscriptionAdminClient;
	public TopicName longTopic, ackTopic;



	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	// For long running jobs
	volatile boolean completed = false, cleanUp = false;
	Stack<PubsubMessage> jobStack = new Stack<PubsubMessage>();
	// Logging
	final static Logger logger = Logger.getLogger(LocalWorkerGoogle.class);

	
	/**
	 *
	 * Constructor for worker node handling long running jobs. 
	 * In this case, the long running job stream is needed.
	 *
	 */

	public LocalWorkerGoogle(String uuid, boolean cleanUp)
	{
		this.uuid = uuid;
		this.cleanUp = cleanUp;
		tempDir = "/tmp/" + uuid;
		ackTopic  = TopicName.create(projectId, uuid);
		longTopic = TopicName.create(projectId, uuid+"-Long-Jobs");
		
		try
		{
			Runtime.getRuntime().exec("mkdir -p " + tempDir);

			subscriptionAdminClient = SubscriptionAdminClient.create();
			setupTopicSubscriber();

			cachedFiles = new ConcurrentHashMap<String, Boolean>();
			int nProc = Runtime.getRuntime().availableProcessors();

			GoogleExecutor executors[] = new GoogleExecutor[nProc];
			for (int i=0; i<nProc; i++)
			{
				executors[i] = new GoogleExecutor(uuid, tempDir, cachedFiles);
				executors[i].setJobStack(jobStack);
				executors[i].start();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void setupTopicSubscriber()
	{
		try
		{

			SubscriptionName subscriptionName = SubscriptionName.create(projectId, uuid + "-Long-Jobs");
			// create a pull subscription with default acknowledgement deadline
			Subscription subscription = subscriptionAdminClient.createSubscription(
				subscriptionName, longTopic, PushConfig.getDefaultInstance(), 0);
			MessageReceiver receiver = new MessageReceiver() 
			{
				@Override
				public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) 
				{
					// handle incoming message, then ack/nack the received message
					jobStack.push(message);
					consumer.ack();
				}
			};
			Subscriber subscriber = null;
			try 
			{
				// Create a subscriber for "my-subscription-id" bound to the message receiver
				subscriber = Subscriber.defaultBuilder(subscriptionName, receiver).build();
				subscriber.startAsync();
			} finally 
			{
				if (subscriber != null) 
				{
				}	
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
				sleep(100);
			} catch (Exception e)
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
