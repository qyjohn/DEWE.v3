package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import org.apache.log4j.Logger;

public class LambdaLocalUploadThread extends Thread
{
	// S3 side requirements
	public AmazonS3Client s3Client;
	// Local requirements
	public String tempDir = "/tmp";
	ConcurrentHashMap<String, Boolean> cachedFiles;
	ConcurrentLinkedQueue<String> uploadQueue;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaLocalUploadThread.class);
	 
	public LambdaLocalUploadThread(String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles, ConcurrentLinkedQueue<String> uploadQueue)
	{
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;
		this.uploadQueue = uploadQueue;

		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		this.s3Client = new AmazonS3Client(clientConfig);
	}

	public void upload_one(String job)
	{
		try
		{
			// The filename comes in the following format
			// bucket|prefix|bin|filename 
			// bucket|prefix|workdir|filename 
			cachedFiles.put(job, new Boolean(false));
			String[] info = job.split("\\|");
			String bucket = info[0];
			String prefix = info[1];
			String folder = info[2];
			String file   = info[3];

			String key  = prefix + "/" + folder + "/" + file;
			String filename = tempDir + "/" + file;

			logger.debug("Uploading " + filename + " to " + key);
			boolean success = false;
			while (!success)
			{
				try
				{
					s3Client.putObject(new PutObjectRequest(bucket, key, new File(filename)));
					cachedFiles.put(job, new Boolean(true));
					success = true;
				} catch (Exception e1)
				{
					logger.error("Error uploading " + file);
					logger.error("Retry after 1000 ms...");
					System.out.println(e1.getMessage());
					e1.printStackTrace();						
					sleep(1000);
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void run()
	{
		while (true)
		{
			try
			{
				String file = uploadQueue.poll();
				if (file != null)
				{
					upload_one(file);
				}
				else
				{
					sleep(1000);
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
