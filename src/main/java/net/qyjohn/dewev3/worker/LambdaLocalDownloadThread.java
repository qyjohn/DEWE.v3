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

public class LambdaLocalDownloadThread extends Thread
{
	// S3 side requirements
	public AmazonS3Client s3Client;
	// Local requirements
	public String tempDir = "/tmp";
	ConcurrentHashMap<String, Boolean> cachedFiles;
	ConcurrentLinkedQueue<String> downloadQueue;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaLocalDownloadThread.class);
	 
	public LambdaLocalDownloadThread(String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles, ConcurrentLinkedQueue<String> downloadQueue)
	{
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;
		this.downloadQueue = downloadQueue;

		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		this.s3Client = new AmazonS3Client(clientConfig);
	}

	public void download_one(String job)
	{
		try
		{
			if (cachedFiles.get(job) == null)
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
				String key    = prefix + "/" + folder + "/" + file;
				String outfile= tempDir + "/" + file;
		
				// Download until success
//				logger.debug("Downloading " + job);
				logger.debug("Downloading " + key + " to " + outfile);
				boolean success = false;
				while (!success)
				{
					try
					{
						S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));
						InputStream in = object.getObjectContent();
						OutputStream out = new FileOutputStream(outfile);
	
						int read = 0;
						byte[] bytes = new byte[1024];
						while ((read = in.read(bytes)) != -1) 
						{
							out.write(bytes, 0, read);
						}
						in.close();
						out.close();


						if (folder.equals("bin"))	// Binary file
						{
							Process p = Runtime.getRuntime().exec("chmod +x " + outfile);
							p.waitFor();
						}

						success = true;
					} catch (Exception e1)
					{
						logger.error("Error downloading " + outfile);
						logger.error("Retry after 1000 ms... ");
						System.out.println(e1.getMessage());
						e1.printStackTrace();
						sleep(1000);
					}
				}
				cachedFiles.put(job, new Boolean(true));
			}
			else
			{
				while (cachedFiles.get(job).booleanValue() == false)
				{
					try
					{
						sleep(100);
					} catch (Exception e)
					{
						System.out.println(e.getMessage());
						e.printStackTrace();
					}
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
				String file = downloadQueue.poll();
				if (file != null)
				{
					download_one(file);
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
