package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
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

public class DeweExecutor extends Thread
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	Stack<String> jobStack;
	// Cache binary and input / output data
	public boolean caching = false;
	// Logging
	final static Logger logger = Logger.getLogger(DeweExecutor.class);

	public DeweExecutor(AmazonS3Client s3Client, AmazonKinesisClient kinesisClient, String tempDir)
	{
		this.s3Client = s3Client;
		this.kinesisClient = kinesisClient;
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;

		// No caching, create a new temp folder for each invocation
		runCommand("mkdir -p " + this.tempDir);
	}

	 
	public DeweExecutor(AmazonS3Client s3Client, AmazonKinesisClient kinesisClient, String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles)
	{
		this.s3Client = s3Client;
		this.kinesisClient = kinesisClient;
		this.tempDir = tempDir;
		this.cachedFiles = cachedFiles;
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
					sleep(new Random().nextInt(100));
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
				logger.debug(jobXML);
				Element job = DocumentHelper.parseText(jobXML).getRootElement();
				workflow = job.attributeValue("workflow");
				bucket   = job.attributeValue("bucket");
				prefix   = job.attributeValue("prefix");
				jobId    = job.attributeValue("id");
				jobName  = job.attributeValue("name");
				command  = tempDir + "/" + jobName;
				logger.info(jobId + ":\t" + jobName);
				// Compose the command to execute
				Element args = job.element("argument");
				Node node;
				Element e;
				StringTokenizer st;
				for ( int i = 0, size = args.nodeCount(); i < size; i++ )
				{
					node = args.node(i);
					if ( node instanceof Element ) 
					{
						e = (Element) node;
						command = command + " " + e.attribute("name").getValue();
					}
					else
					{
						st = new StringTokenizer(node.getText().trim());
						while (st.hasMoreTokens()) 
						{
							command = command + " " + st.nextToken();
						}
				    	}
				}
				logger.debug(command);

				// Input and output file definitions
				HashSet<String> exeFiles  = new HashSet<String>();
				HashSet<String> inFiles  = new HashSet<String>();
				HashSet<String> outFiles = new HashSet<String>();
				exeFiles.add(jobName);
				for ( Iterator iter = job.elementIterator( "uses" ); iter.hasNext(); ) 
				{
					Element file = (Element) iter.next();
					if (file.attribute("link").getValue().equals("input"))
					{
						// This is an input file
						if (file.attribute("executable") != null)
						{
							if (file.attribute("executable").getValue().equals("true"))
							{
								exeFiles.add(file.attribute("name").getValue());							
							}
							else
							{
								inFiles.add(file.attribute("name").getValue());
							}							
						}
						else
						{
							inFiles.add(file.attribute("name").getValue());
						}							
					}
					else
					{
						outFiles.add(file.attribute("name").getValue());
					}
				}

				// Download the executable and the input files
				for (String f : exeFiles)
				{
					download(1, f);
					runCommand("chmod u+x " + tempDir + "/" + f);
				}
				for (String f : inFiles)
				{
					download(2, f);
				}

				// Execute the command and wait for it to complete
				String env_path = "PATH=$PATH:" + tempDir;
				String env_lib = "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:" + tempDir;
				String[] env = {env_path, env_lib};
				Process p2 = Runtime.getRuntime().exec(command, env, new File(tempDir));
				BufferedReader in = new BufferedReader(new InputStreamReader(p2.getInputStream()));
				String result = "";
				String line;
				while ((line = in.readLine()) != null) 
				{
					result = result + line + "\n";
				}       
				in.close();
				p2.waitFor();
				logger.debug(result);

				// Upload the output files
				for (String f : outFiles)
				{
					upload(f);
				}

				// Acknowledge the job to be completed
				ackJob(workflow, jobId);

				// If no caching, clean up after execution
				if (!caching)
				{
					runCommand("rm -Rf " + tempDir);
				}		
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
	}
	
	/**
	 *
	 * Download binary and input data from S3 to the execution folder.
	 *
	 */
	 
	public void download(int type, String filename)
	{
		if (caching)
		{
			if (cachedFiles.get(filename) == null)
			{
				cachedFiles.put(filename, new Boolean(false));
				String key=null, outfile = null;
				if (type==1)	// Binary
				{
					key = prefix + "/bin/" + filename;
					outfile = tempDir + "/" + filename;
				}
				else	// Data
				{
					key = prefix + "/workdir/" + filename;
					outfile = tempDir + "/" + filename;
				}
		
				try
				{
					logger.debug("Downloading " + outfile);
					S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));
					InputStream in = object.getObjectContent();
					OutputStream out = new FileOutputStream(outfile);
					IOUtils.copy(in, out);
					in.close();
					out.close();
					object.close();
				} catch (Exception e)
				{
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
				cachedFiles.put(filename, new Boolean(true));
			}
			else
			{
				while (cachedFiles.get(filename).booleanValue() == false)
				{
					try
					{
						sleep(50);
					} catch (Exception e)
					{
						System.out.println(e.getMessage());
						e.printStackTrace();
					}
				}
			}			
		}
		else
		{
				String key=null, outfile = null;
				if (type==1)	// Binary
				{
					key = prefix + "/bin/" + filename;
					outfile = tempDir + "/" + filename;
				}
				else	// Data
				{
					key = prefix + "/workdir/" + filename;
					outfile = tempDir + "/" + filename;
				}
		
				try
				{
					logger.debug("Downloading " + outfile);
					S3Object object = s3Client.getObject(new GetObjectRequest(bucket, key));
					InputStream in = object.getObjectContent();
					OutputStream out = new FileOutputStream(outfile);
					IOUtils.copy(in, out);
					in.close();
					out.close();
				} catch (Exception e)
				{
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
			
		}
	}

	/**
	 *
	 * Upload output data to S3
	 *
	 */
	 
	public void upload(String filename)
	{
		if (caching)
		{
			cachedFiles.put(filename, new Boolean(false));
		}
		String key  = prefix + "/workdir/" + filename;
		String file = tempDir + "/" + filename;

		try
		{
			logger.debug("Uploading " + file);
			s3Client.putObject(new PutObjectRequest(bucket, key, new File(file)));
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		if (caching)
		{
			cachedFiles.put(filename, new Boolean(true));
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
