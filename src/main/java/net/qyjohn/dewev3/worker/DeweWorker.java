package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
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

public class DeweWorker extends Thread
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	public LinkedList<String> cachedFiles = new LinkedList<String>();
	// For long running jobs
	volatile boolean completed = false;
	String longStream;
	List<Shard> longShards = new ArrayList<Shard>();
	Map<String, String> longIterators = new HashMap<String, String>();
	boolean cleanUp = false;
	// Logging
	final static Logger logger = Logger.getLogger(DeweWorker.class);

	/**
	 *
	 * Constructor for Lambda function. 
	 * In this case, the long running job stream is not needed.
	 *
	 */
	 
	public DeweWorker()
	{
		s3Client = new AmazonS3Client();
		kinesisClient = new AmazonKinesisClient();
	}
	
	/**
	 *
	 * Constructor for worker node handling long running jobs. 
	 * In this case, the long running job stream is needed.
	 *
	 */

	public DeweWorker(String longStream, boolean cleanUp)
	{
		try
		{
			this.longStream = longStream;
			this.cleanUp = cleanUp;
			tempDir = "/tmp/" + longStream;
			Process p = Runtime.getRuntime().exec("mkdir -p " + tempDir);
			p.waitFor();

			s3Client = new AmazonS3Client();
			kinesisClient = new AmazonKinesisClient();
			listLongShards();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 *
	 * The long running job handler receives jobs from a separate Kinesis stream.
	 *
	 */

	public void listLongShards()
	{
		DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
		describeStreamRequest.setStreamName(longStream);
		String exclusiveStartShardId = null;
		do 
		{
			describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
			DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
			longShards.addAll( describeStreamResult.getStreamDescription().getShards() );
			if (describeStreamResult.getStreamDescription().getHasMoreShards() && longShards.size() > 0) 
			{
				exclusiveStartShardId = longShards.get(longShards.size() - 1).getShardId();
			} 
			else 
			{
				exclusiveStartShardId = null;
			}
		} while ( exclusiveStartShardId != null );

		for (Shard shard : longShards)
		{
			String shardId = shard.getShardId();
			String shardIterator;
			GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
			getShardIteratorRequest.setStreamName(longStream);
			getShardIteratorRequest.setShardId(shardId);
			getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

			GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
			shardIterator = getShardIteratorResult.getShardIterator();			
			longIterators.put(shardId, shardIterator);
		}
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
				executeJob(jobXML);
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

	
	/**
	 *
	 * When the job handler runs on an EC2 instance, it is a long running thread.
	 *
	 */
	 
	public void run()
	{
		while (!completed)
		{
			try
			{
				// Listen for longStream for jobs to execute
				for (Shard shard : longShards)
				{
					String shardId = shard.getShardId();
					GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
					getRecordsRequest.setShardIterator(longIterators.get(shardId));
					getRecordsRequest.setLimit(100);
	
					GetRecordsResult getRecordsResult = kinesisClient.getRecords(getRecordsRequest);
					List<Record> records = getRecordsResult.getRecords();
					for (Record record : records)
					{
						String jobXML = new String(record.getData().array());
						executeJob(jobXML);
					}
	
					longIterators.put(shardId, getRecordsResult.getNextShardIterator());
				}
			} catch (ResourceNotFoundException e)
			{
				// The longStream has been deleted. The workflow has completed execution
				completed = true;
			}
		}

		// Remove temp folder
		if (cleanUp)
		{
			try
			{
				Process p = Runtime.getRuntime().exec("rm -Rf " + tempDir);
				p.waitFor();
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}		
		}
	}
	
	
	/**
	 *
	 * Mark the workflow as completed. This is used for the EC2 job handler to exit gracefully.
	 *
	 */
	 
	public void setAsCompleted()
	{
		completed = true;
	}
	
	
	/**
	 *
	 * The method to execute a single job.
	 *
	 */

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
					Process p1 = Runtime.getRuntime().exec("chmod u+x " + tempDir + "/" + f);
					p1.waitFor();
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
		if (!cachedFiles.contains(filename))
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
			cachedFiles.add(filename);
		}
	}

	/**
	 *
	 * Upload output data to S3
	 *
	 */
	 
	public void upload(String filename)
	{
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

		cachedFiles.add(filename);		
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
	

}
