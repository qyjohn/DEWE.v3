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

public class LambdaHandler
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonSQSClient sqsClient;
	public String workflow, bucket, prefix, jobId, jobName, command;
	// Cached binaries, input and output files
	public String tempDir, ackQueue;
	public HashMap<String, Boolean> cachedFiles;

	// Logging
	final static Logger logger = Logger.getLogger(LambdaHandler.class);


	/**
	 *
	 * The only purpose of this handler is to clean up the /tmp disk space.
	 *
	 */

	public void cleanUpHandler(KinesisEvent event)
	{
		runCommand("rm -Rf /tmp/*", "/tmp");
	}
	

	public void parallelHandler(KinesisEvent event)
	{
		ClientConfiguration clientConfig = new ClientConfiguration();
		clientConfig.setMaxConnections(1000);
		clientConfig.setSocketTimeout(60*1000);
		s3Client = new AmazonS3Client(clientConfig);
		sqsClient = new AmazonSQSClient();

		// Create temporary execution folder
		tempDir = "/tmp/" + UUID.randomUUID().toString();
		runCommand("mkdir -p " + tempDir, "/tmp");
		
		// Create the data structure to store all job id's, commands, binaries, input and output files
		List<KinesisEvent.KinesisEventRecord> records = event.getRecords();
		List<String> ids      = new LinkedList<String>();
		List<String> commands = new LinkedList<String>();
		List<String> binFiles = new LinkedList<String>();
		List<String> inFiles  = new LinkedList<String>();
		List<String> outFiles = new LinkedList<String>();	
			
		// Extract infomation about all job id's, commands, binaries, input and output files
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
				ids.add(job.attributeValue("id"));
				commands.add(job.attributeValue("command"));
				
				// Binaries, input and output files
				StringTokenizer st;
				st = new StringTokenizer(job.attribute("binFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
					if (!binFiles.contains(f))
					{
						binFiles.add(f);
					}
				}
				st = new StringTokenizer(job.attribute("inFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
					if (!inFiles.contains(f))
					{
						inFiles.add(f);
					}
				}
				st = new StringTokenizer(job.attribute("outFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
					if (!outFiles.contains(f))
					{
						outFiles.add(f);
					}
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
		
		// Parallel download of binaries using multiple threads
		if (!binFiles.isEmpty())
		{
			try
			{
				Downloader downloader[] = new Downloader[binFiles.size()];
				for (int i=0; i<binFiles.size(); i++)
				{
					downloader[i] = new Downloader("bin", binFiles.get(i));
					downloader[i].start();
				}
				for (int i=0; i<binFiles.size(); i++)
				{
					downloader[i].join();
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}		
		
		// Parallel donwload of input files using multiple threads
		if (!inFiles.isEmpty())
		{
			try
			{
				Downloader downloader[] = new Downloader[inFiles.size()];
				for (int i=0; i<inFiles.size(); i++)
				{
					downloader[i] = new Downloader("workdir", inFiles.get(i));
					downloader[i].start();
				}
				for (int i=0; i<inFiles.size(); i++)
				{
					downloader[i].join();
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}		
		
		// "chmod +x" for all binaries
		String cmd = "chmod +x";
		for (String c : binFiles)
		{
			cmd = cmd + " " + c;
		}
		runCommand(cmd, tempDir);
		
		// Parallel execution of commands using multiple threads
		try
		{
			if (!commands.isEmpty())
			{
				Executor executor[] = new Executor[commands.size()];
				for (int i=0; i<commands.size(); i++)
				{
					executor[i] = new Executor(tempDir + "/" + commands.get(i));
					executor[i].start();
				}
				for (int i=0; i<commands.size(); i++)
				{
					executor[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// Parallel upload of output files using multiple threads
		try
		{
			if (!outFiles.isEmpty())
			{
				Uploader uploader[] = new Uploader[outFiles.size()];
				for (int i=0; i<outFiles.size(); i++)
				{
					uploader[i] = new Uploader(outFiles.get(i));
					uploader[i].start();
				}
				for (int i=0; i<outFiles.size(); i++)
				{
					uploader[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// Parallel ACK of all jobs using multiple threads
		try
		{
			if (!ids.isEmpty())
			{
				Acker acker[] = new Acker[ids.size()];
				for (int i=0; i<ids.size(); i++)
				{
					acker[i] = new Acker(ids.get(i));
					acker[i].start();
				}
				for (int i=0; i<ids.size(); i++)
				{
					acker[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// Clean up temporary execution folder
		runCommand("rm -Rf " + tempDir, "/tmp");
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
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
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
			try
			{
				String key     = prefix + "/" + folder + "/" + filename;
				String outfile = tempDir + "/" + filename;
		
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
						success = true;
					} catch (Exception e1)
					{
						logger.error("Error downloading " + outfile);
						logger.error("Retry after 200 ms... ");
						System.out.println(e1.getMessage());
						e1.printStackTrace();
						sleep(200);
					}
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
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
			try
			{
				String key  = prefix + "/workdir/" + filename;
				String file = tempDir + "/" + filename;

				logger.debug("Uploading " + file);
				boolean success = false;
				while (!success)
				{
					try
					{
						s3Client.putObject(new PutObjectRequest(bucket, key, new File(file)));
						success = true;
					} catch (Exception e1)
					{
						logger.error("Error uploading " + file);
						logger.error("Retry after 200 ms...");
						System.out.println(e1.getMessage());
						e1.printStackTrace();						
						sleep(200);
					}
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}	
	
	class Executor extends Thread
	{
		public String cmd, dir;
		
		public Executor(String cmd)
		{
			this.cmd = cmd;
		}
		
		public void run()
		{
			try
			{
				runCommand(cmd, tempDir);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}			
		}
	}
	class Acker extends Thread
	{
		public String id;
		
		public Acker(String id)
		{
			this.id = id;
		}
		
		public void run()
		{
			try
			{
				logger.info("ACK " + id);
				sqsClient.sendMessage(ackQueue, id);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
