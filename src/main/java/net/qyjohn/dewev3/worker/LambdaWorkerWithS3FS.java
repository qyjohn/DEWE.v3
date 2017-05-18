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

public class LambdaWorkerWithS3FS
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String workflow, bucket, prefix, jobId, jobName, command;
	//  S3FS
	public String s3FsDir = "/tmp/s3mnt";
	public boolean s3FsReady = false;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaWorkerWithS3FS.class);

	/**
	 *
	 * Constructor for Lambda function. 
	 * In this case, the long running job stream is not needed.
	 *
	 */
	 
	public LambdaWorkerWithS3FS()
	{
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(100);
		s3Client = new AmazonS3Client(clientConfig);
		kinesisClient = new AmazonKinesisClient();
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
				logger.info(jobXML);
				executeJob(jobXML);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
	
	public void executeJob(String jobXML)
	{
		try
		{
			Element job = DocumentHelper.parseText(jobXML).getRootElement();
			workflow = job.attributeValue("workflow");
			bucket   = job.attributeValue("bucket");
			prefix   = job.attributeValue("prefix");
			jobId    = job.attributeValue("id");
			jobName  = job.attributeValue("name");

			if (!s3FsReady)
			{
				mountS3Fs(bucket);
			}
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}
	
	
	public void mountS3Fs(String bucket)
	{
		try
		{
			logger.info("Attempting to mount S3FS.");
			// Create the mounting point
			runCommand("mkdir -p " + s3FsDir);
			
			// Extract the s3fs binary
			String s3FsBin = "/tmp/s3fs";
			InputStream in = getClass().getResourceAsStream("/s3fs"); 
			OutputStream out = new FileOutputStream(s3FsBin);
			IOUtils.copy(in, out);
			in.close();
			out.close();
			
			// Make the s3fs binary executable
			runCommand("chmod u+x " + s3FsBin);			
			// Mount S3FS
			String mount = "/tmp/s3fs -o complement_stat -o umask=000 " + bucket + " " + s3FsDir;
			runCommand(mount);
						
			// Mark S3FS as ready
			s3FsReady = true;
			
			// Test S3FS
			runCommand("ls /tmp");
			runCommand("ls " + s3FsDir);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}
	
	public void runCommand(String command)
	{
		try
		{
			logger.info(command);
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String result = "";
			String line;
			while ((line = in.readLine()) != null) 
			{
				result = result + line + "\n";
			}       
			in.close();
			p.waitFor();
			logger.info(result);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}

}
