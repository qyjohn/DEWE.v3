package net.qyjohn.dewev3.worker;

/**
 *
 * net.qyjohn.dewev3.worker.LambdaHandlerBase::deweHandler
 *
 * Everything in /tmp/dewe, no caching, serial download, serial execution
 *
 */

import java.io.*;
import java.nio.*;
import java.nio.file.*;
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

public class LambdaHandlerBase
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonSQSClient sqsClient;
	public String workflow, bucket, prefix, ackQueue;
	public int MAX_RETRY = 5;
	public String deweDir = "/tmp/dewe";
	// Common job definitions
	LinkedList<String> commands, jobs, binFiles, inFiles, outFiles;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaHandlerV2.class);

	public void deweHandler(KinesisEvent event)
	{
		prepareJobs(event);
		createEnv();
		executeJobs();
		cleanUp();
	}
	
	
	public void prepareJobs(KinesisEvent event)
	{
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		s3Client = new AmazonS3Client(clientConfig);
		sqsClient = new AmazonSQSClient();

		List<KinesisEvent.KinesisEventRecord> records = event.getRecords();
		commands = new LinkedList<String>();
		jobs     = new LinkedList<String>();
		binFiles = new LinkedList<String>();
		inFiles  = new LinkedList<String>();
		outFiles = new LinkedList<String>();
		
		for(KinesisEvent.KinesisEventRecord rec : records)
		{
			try
			{
				// Basic workflow information
				String jobXML = new String(rec.getKinesis().getData().array());
				Element job = DocumentHelper.parseText(jobXML).getRootElement();
				workflow = job.attributeValue("workflow");
				bucket   = job.attributeValue("bucket");
				prefix   = job.attributeValue("prefix");
				ackQueue = job.attributeValue("ackQueue");
				
				// Job definitions, binaries, input and output files
				jobs.add(job.attributeValue("id"));
				commands.add(job.attributeValue("command"));
				StringTokenizer st;
				st = new StringTokenizer(job.attribute("binFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String t = st.nextToken();
					if (!binFiles.contains(t))
					{
						binFiles.add(t);						
					}
				}
				st = new StringTokenizer(job.attribute("inFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String t = st.nextToken();
					if (!inFiles.contains(t))
					{
						inFiles.add(t);						
					}
				}
				st = new StringTokenizer(job.attribute("outFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String t = st.nextToken();
					if (!outFiles.contains(t))
					{
						outFiles.add(t);						
					}
				}		
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	/**
	 *
	 * Run a command 
	 *
	 */
	 
	public void runCommand(String command, String dir)
	{
		try
		{
			logger.info(command);

			String env_path = "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:" + deweDir;
			String env_lib = "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" + deweDir;
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
			logger.info(result);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}
	
	/**
	 *
	 * Download a file
	 *
	 */
	
	public void download_one(String folder, String filename, String dir)
	{
		try
		{
			String key     = prefix + "/" + folder + "/" + filename;
			String outfile = dir + "/" + filename;
		
			logger.info("Downloading " + key + " to " + outfile);
			boolean success = false;
			int retry = 0;
						
			while ((!success) && (retry < MAX_RETRY))
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
								
					if (folder.equals("bin"))
					{
						runCommand("chmod +x " + outfile, deweDir);
					}								
				} catch (Exception e1)
				{
					retry++;
					logger.error("Error downloading " + outfile);
					logger.error("Retry after 1000 ms... ");
					System.out.println(e1.getMessage());
					e1.printStackTrace();
					Thread.sleep(1000);
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
	 * Upload a file 
	 *
	 */

	public void upload_one(String filename)
	{
		try
		{
			String key  = prefix + "/workdir/" + filename;
			String file = deweDir + "/" + filename;

			logger.debug("Uploading " + file);
			boolean success = false;
			int retry = 0;
						
			while ((!success) && (retry < MAX_RETRY))
			{
				try
				{
					s3Client.putObject(new PutObjectRequest(bucket, key, new File(file)));
					success = true;
				} catch (Exception e1)
				{
					retry++;
					logger.error("Error uploading " + file);
					logger.error("Retry after 1000 ms...");
					System.out.println(e1.getMessage());
					e1.printStackTrace();	
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
	 * Create the execution folder 
	 *
	 */

	public void createEnv()
	{
		runCommand("mkdir -p " + deweDir, "/tmp");
	}

 	/**
	 *
	 * Execute all the jobs
	 *
	 */

	public void executeJobs()
	{
		// Download binaries and input files
		for (String f : binFiles)
		{
			download_one("bin", f, deweDir);
		}
		for (String f : inFiles)
		{
			download_one("workdir", f, deweDir);
		}
				
		// Run all commands
		for (String com : commands)
		{
			runCommand(deweDir + "/" + com, deweDir);	
		}		
				
		// Upload output files
		for (String f : outFiles)
		{
			upload_one(f);
		}		
					
		// ACK all jobs
		for (String id : jobs)
		{
			sqsClient.sendMessage(ackQueue, id);
		}		
		
	}
	
	
 	/**
	 *
	 * Clean up execution folder
	 *
	 */

	public void cleanUp()	
	{
		runCommand("rm -Rf " + deweDir, "/tmp");				
	}	
}
