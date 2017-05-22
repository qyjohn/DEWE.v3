package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import net.qyjohn.dewev3.worker.*;
import org.apache.log4j.Logger;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.pubsub.v1.*;

import com.google.protobuf.ByteString;
import com.google.gson.Gson;


public class GoogleTransceiver
{
	public String projectId = ServiceOptions.getDefaultProjectId();
	public TopicAdminClient topicAdminClient;
	public SubscriptionAdminClient subscriptionAdminClient;
	public TopicName jobTopic, longTopic, ackTopic;
	public Publisher jobSender, longSender;
	public Subscriber ackReceiver;
	public Stack<String> ackStack = new Stack<String>();
	final static Logger logger = Logger.getLogger(GoogleTransceiver.class);
	
	/**
	 *
	 * Constructor
	 * Setup the corresponding topics and publisher / subscriber
	 *
	 */

	public GoogleTransceiver(String uuid, String topic)
	{
		try
		{
			// Topic names
			jobTopic  = TopicName.create(projectId, topic);
            ackTopic  = TopicName.create(projectId, uuid);
			longTopic = TopicName.create(projectId, uuid+"-Long-Jobs");
			
			// The jobTopic is a pre-existing topic.
			// We need to create the ackTopic and longTopic on the fly
			topicAdminClient = TopicAdminClient.create();
			subscriptionAdminClient = SubscriptionAdminClient.create();
			topicAdminClient.createTopic(ackTopic);
			topicAdminClient.createTopic(longTopic);

			// Create the jobSender and longSender, and ackReceiver
			jobSender = Publisher.defaultBuilder(jobTopic).build();
			longSender= Publisher.defaultBuilder(longTopic).build();

			SubscriptionName subscriptionName = SubscriptionName.create(projectId, uuid);
			// create a pull subscription with default acknowledgement deadline
			Subscription subscription = subscriptionAdminClient.createSubscription(
				subscriptionName, ackTopic, PushConfig.getDefaultInstance(), 0);
			MessageReceiver receiver = new MessageReceiver() 
			{
				@Override
				public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) 
				{
					ackStack.push(message.getData().toStringUtf8().replaceAll("^\"|\"$", ""));	//Remove double quotes
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
//					subscriber.stopAsync();
				}	
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	

	/*
	 *
	 * Delete the temporary topics when the workflow is completed.
	 *
	 */
	
	public void cleanUp()
	{
		topicAdminClient.deleteTopic(ackTopic); 
		topicAdminClient.deleteTopic(longTopic);	
	}
	
	
	
	/**
	 *
	 * Publishing a job to the corresponding topic.
	 *
	 */
	 
	public void publishJob(WorkflowJob job)
	{
		logger.info("Dispatching " + job.jobId + ":\t" + job.jobName);
		Gson gson = new Gson();
		String binFiles = "";
		for (String s : job.binFiles)
		{
			binFiles = binFiles + s + " ";
		}
		String inFiles = "";
		for (String s : job.inFiles)
		{
			inFiles = inFiles + s + " ";
		}
		String outFiles = "";
		for (String s : job.outFiles)
		{
			outFiles = outFiles + s + " ";
		}

		try
		{
			PubsubMessage message = PubsubMessage.newBuilder()
				.putAttributes("workflow", job.workflow)
				.putAttributes("bucket", job.bucket)
				.putAttributes("prefix", job.prefix)
				.putAttributes("id", job.jobId)
				.build();
				
			if (job.isLongJob)
			{
				longSender.publish(message);
			}
			else
			{
				jobSender.publish(message);
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	
	/**
	 *
	 * Receive job completion information from the ackTopic.
	 *
	 */

	public String receiveAck()
	{
		if (ackStack.empty())
		{
			return null;
		}
		else
		{
			return ackStack.pop();
		}
	}
}

