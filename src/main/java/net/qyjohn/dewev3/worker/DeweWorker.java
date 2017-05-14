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

public class DeweWorker extends Thread
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	// Long running jobs
	volatile boolean completed = false;
	String longStream;
	List<Shard> longShards = new ArrayList<Shard>();
	Map<String, String> longIterators = new HashMap<String, String>();

	public DeweWorker()
	{
		s3Client = new AmazonS3Client();
		kinesisClient = new AmazonKinesisClient();
	}
	
	public DeweWorker(String longStream)
	{
		this.longStream = longStream;
		s3Client = new AmazonS3Client();
		kinesisClient = new AmazonKinesisClient();
		listLongShards();
	}
	
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
					// For each job, create a temp folder under /tmp
					tempDir = tempDir + "/" + UUID.randomUUID().toString();
					executeJob(jobXML);
				}

				longIterators.put(shardId, getRecordsResult.getNextShardIterator());
			}
		}		
	}
	
	public void setAsCompleted()
	{
		completed = true;
	}
	
	public void executeJob(String jobXML)
	{
			try
			{
				Process p0 = Runtime.getRuntime().exec("mkdir -p " + tempDir);
				p0.waitFor();

				System.out.println(jobXML);
				Element job = DocumentHelper.parseText(jobXML).getRootElement();
				workflow = job.attributeValue("workflow");
				bucket   = job.attributeValue("bucket");
				prefix   = job.attributeValue("prefix");
				jobId    = job.attributeValue("id");
				jobName  = job.attributeValue("name");
				command  = tempDir + "/" + jobName;

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
						command = command + " " + e.attribute("file").getValue();
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
				System.out.println(command);

				// Input and output file definitions
				HashSet<String> inFiles  = new HashSet<String>();
				HashSet<String> outFiles = new HashSet<String>();
				for ( Iterator iter = job.elementIterator( "uses" ); iter.hasNext(); ) 
				{
					Element file = (Element) iter.next();
					System.out.println(file.attribute("file").getValue() + "\t" + file.attribute("link").getValue());
					if (file.attribute("link").getValue().equals("input"))
					{
						inFiles.add(file.attribute("file").getValue());
					}
					else
					{
						outFiles.add(file.attribute("file").getValue());
					}
				}

				// Download the executable and the input files
				download(1, jobName);
				Process p1 = Runtime.getRuntime().exec("chmod u+x " + tempDir + "/" + jobName);
				p1.waitFor();
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
				System.out.println(result);

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
	
	public void download(int type, String filename)
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

	public void upload(String filename)
	{
		String key  = prefix + "/workdir/" + filename;
		String file = tempDir + "/" + filename;

		try
		{
			s3Client.putObject(new PutObjectRequest(bucket, key, new File(file)));
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
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
	

}
