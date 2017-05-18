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

public class LambdaWorker
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public DeweExecutor executor;
	public String tempDir = "/tmp";
	// Logging
	final static Logger logger = Logger.getLogger(LambdaWorker.class);

	/**
	 *
	 * Constructor for Lambda function. 
	 * In this case, the long running job stream is not needed.
	 *
	 */
	 
	public LambdaWorker()
	{
		tempDir = "/tmp/" + UUID.randomUUID().toString();
		s3Client = new AmazonS3Client();
		kinesisClient = new AmazonKinesisClient();
		executor = new DeweExecutor(s3Client, kinesisClient, tempDir);
	}

	
	/**
	 *
	 * When the job handler runs on an EC2 instance, it is a function triggered by Lambda.
	 *
	 */

	public void lambdaHandler(KinesisEvent event)
	{
		for(KinesisEvent.KinesisEventRecord rec : event.getRecords())
		{
			try
			{
				// Basic workflow information
				String jobXML = new String(rec.getKinesis().getData().array());
				executor.executeJob(jobXML);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
