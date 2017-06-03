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
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class LambdaLocalExecutor extends Thread
{
	// Common components
	public String ackQueue;
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public AmazonSQSClient sqsClient = new AmazonSQSClient();
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	Stack<String> jobStack;
	// Cache binary and input / output data
	public boolean caching = false;
	// Logging
	final static Logger logger = Logger.getLogger(LambdaLocalExecutor.class);
	public boolean serialS3 = false;
	 
	public LambdaLocalExecutor(String ackQueue, String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles, boolean serialS3)
	{
		this.ackQueue = ackQueue;
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;
		this.serialS3 = serialS3;
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		this.s3Client = new AmazonS3Client(clientConfig);
		caching = true;
	}
	
	public void setJobStack(Stack<String> stack)
	{
		this.jobStack = stack;
	}

	public void run()
	{
		while (true)
		{
			try
			{
				if (!jobStack.empty())
				{
					String jobXML = jobStack.pop();
					executeJob(jobXML);
				}
				else
				{
					sleep(new Random().nextInt(200));
				}
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
			download("bin", job.attribute("binFiles").getValue());
			download("workdir", job.attribute("inFiles").getValue());
			runCommand("chmod u+x " + tempDir + "/" + job.attribute("binFiles").getValue(), tempDir);

			// Execute the command and wait for it to complete
			runCommand(tempDir + "/" + command, tempDir);

			// Upload output files
			upload(job.attribute("outFiles").getValue());
		} catch (Exception e)
		{
			System.out.println(jobXML);
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

		// Acknowledge the job to be completed
		sqsClient.sendMessage(ackQueue, jobId);
	}

	
	
	/**
	 *
	 * Download binary and input data from S3 to the execution folder.
	 *
	 */
	
	public void download(String folder, String files)
	{
		try
		{
			// Extract all files to download
			StringTokenizer st;
			st = new StringTokenizer(files);
			List<String> list = new ArrayList<String> ();
			while (st.hasMoreTokens()) 
			{
				list.add(st.nextToken());
			}
			// Download in a mutlti-thread fashion
			if (!list.isEmpty())
			{
				if (!serialS3)
				{
					Downloader downloader[] = new Downloader[list.size()];
					for (int i=0; i<list.size(); i++)
					{
						downloader[i] = new Downloader(folder, list.get(i));
						downloader[i].start();
					}
					for (int i=0; i<list.size(); i++)
					{
						downloader[i].join();
					}					
				}	
				else
				{
					for (int i=0; i<list.size(); i++)
					{
						download_one(folder, list.get(i));
					}					
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public void download_one(String folder, String filename)
	{
			try
			{
				if (cachedFiles.get(filename) == null)
				{
					cachedFiles.put(filename, new Boolean(false));
					String key     = prefix + "/" + folder + "/" + filename;
					String outfile = tempDir + "/" + filename;
		
					// Download until success
					logger.debug("Downloading " + outfile);
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
					cachedFiles.put(filename, new Boolean(true));
				}
				else
				{
					while (cachedFiles.get(filename).booleanValue() == false)
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
	
	class Downloader extends Thread
	{
		String folder, filename;

		public Downloader(String folder, String filename)
		{
			this.folder = folder;
			this.filename = filename;
		}

		public void run()
		{
			download_one(folder, filename);
		}
	}
 
	/**
	 *
	 * Upload output data to S3
	 *
	 */
	 
	public void upload(String files) 
	{
		try
		{
			// Extract all files to upload
			StringTokenizer st;
			st = new StringTokenizer(files);
			List<String> list = new ArrayList<String> ();
			while (st.hasMoreTokens()) 
			{
				list.add(st.nextToken());
			}
			// Upload in a mutlti-thread fashion
			if (!list.isEmpty())
			{
				if (!serialS3)
				{
					Uploader uploader[] = new Uploader[list.size()];
					for (int i=0; i<list.size(); i++)
					{
						uploader[i] = new Uploader(list.get(i));
						uploader[i].start();
					}
					for (int i=0; i<list.size(); i++)
					{
						uploader[i].join();
					}					
				}
				else
				{
					for (int i=0; i<list.size(); i++)
					{
						upload_one(list.get(i));
					}		
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	public void upload_one(String filename)
	{
			try
			{
				cachedFiles.put(filename, new Boolean(false));
				String key  = prefix + "/workdir/" + filename;
				String file = tempDir + "/" + filename;

				logger.debug("Uploading " + file);
				boolean success = false;
				while (!success)
				{
					try
					{
						s3Client.putObject(new PutObjectRequest(bucket, key, new File(file)));
						cachedFiles.put(filename, new Boolean(true));
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
	class Uploader extends Thread
	{
		public String filename;

		public Uploader(String filename)
		{
			this.filename = filename;
		}

		public void run()
		{
			upload_one(filename);
		}

	}

	/**
	 *
	 * ACK to the workflow scheduler that the job is now completed
	 *
	 */
	 
	public void ackJob(String ackStream, String id)
	{
			byte[] bytes = id.getBytes();
			PutRecordRequest putRecord = new PutRecordRequest();
			putRecord.setStreamName(ackStream);
			putRecord.setPartitionKey(UUID.randomUUID().toString());
			putRecord.setData(ByteBuffer.wrap(bytes));

			try 
			{
				kinesisClient.putRecord(putRecord);
			} catch (Exception e) 
			{
				System.out.println(e.getMessage());
				e.printStackTrace();	
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
