package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.lambda.runtime.*; 
import com.amazonaws.services.lambda.runtime.events.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class LambdaLocalExecutorV2 extends Thread
{
	// Common components
	public String ackQueue;
	public AmazonS3Client s3Client;
	public AmazonSQSClient sqsClient = new AmazonSQSClient();
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	public ConcurrentLinkedQueue<String> jobQueue;
	ConcurrentLinkedQueue<String> downloadQueue;
	ConcurrentLinkedQueue<String> uploadQueue;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaLocalExecutorV2.class);
	public boolean serialS3 = false;
	 
	public LambdaLocalExecutorV2(String ackQueue, String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles)
	{
		this.ackQueue = ackQueue;
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		this.s3Client = new AmazonS3Client(clientConfig);
	}
	
	public void setQueues(ConcurrentLinkedQueue<String> jobQueue, ConcurrentLinkedQueue<String> downloadQueue, ConcurrentLinkedQueue<String> uploadQueue)
	{
		this.jobQueue  = jobQueue;
		this.downloadQueue = downloadQueue;
		this.uploadQueue = uploadQueue;
	}

	public void run()
	{
		while (true)
		{
			try
			{
				String jobXML = jobQueue.poll();
				if (jobXML != null)
				{
					executeJob(jobXML);
				}
				else
				{
					sleep(500);
				}
/*				if (!jobQueue.isEmpty())
				{
					String jobXML = jobQueue.poll();
					if (jobXML != null)
					{
						executeJob(jobXML);
					}
				}
				else
				{
					sleep(1000);
				}
*/
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
			bucket   = job.attributeValue("bucket");
			prefix   = job.attributeValue("prefix");
			jobId    = job.attributeValue("id");
			jobName  = job.attributeValue("name");
			command  = job.attributeValue("command");

			logger.info(jobId + "\t" + jobName);
			logger.debug(jobXML);

			// Download binary and input files
			// Extract all files to download
			List<String> downloadList = new ArrayList<String> ();
			List<String> uploadList   = new ArrayList<String> ();
			StringTokenizer st;
			st = new StringTokenizer(job.attribute("binFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				downloadList.add(bucket + "|" + prefix + "|bin|" + st.nextToken());
				
			}
			st = new StringTokenizer(job.attribute("inFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				downloadList.add(bucket + "|" + prefix + "|workdir|" + st.nextToken());
			}
			st = new StringTokenizer(job.attribute("outFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				uploadList.add(bucket + "|" + prefix + "|workdir|" + st.nextToken());
			}

			// Download all binaries and input files
			for (String file : downloadList)
			{
				downloadQueue.add(file);
			}
			waitFor(downloadList);
			
			// Execute the command and wait for it to complete
			runCommand(tempDir + "/" + command, tempDir);
			
			// Upload all output files
			for (String file : uploadList)
			{
				uploadQueue.add(file);
			}
			waitFor(uploadList);

			// Acknowledge the job to be completed
			sqsClient.sendMessage(ackQueue, jobId);
		} catch (Exception e)
		{
			System.out.println(jobXML);
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}


	public void waitFor(List<String> files)
	{
		boolean done = false;
		while (!done)
		{
			// Assumes that things are done until we find out it is not true
			done = true;
			for (String file : files)
			{
				Boolean state = cachedFiles.get(file);
				if (state == null)	// Not started yet
				{
					done = false;
				}
				else if (state.booleanValue() == false) // Not finished yet
				{
					done = false;
				}
				try	// take a break
				{
					sleep(50);
				} catch (Exception e){}
			}
		}
	}

	
	/**
	 *
	 * Run a command 
	 *
	 */
	 
	public void runCommand(String command, String dir) throws Exception
	{ 
			logger.debug(command);

			String env_path = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:" + dir;
			String env_lib = "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" + dir;
			String[] env = {env_path, env_lib};
			Process p = Runtime.getRuntime().exec(command, env, new File(dir));
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String result = "";
			String line;
			while ((line = in.readLine()) != null) 
			{
				result = result + line + "\n";
			}       
			in.close();
			p.waitFor();
			logger.debug(result);
	}
}
