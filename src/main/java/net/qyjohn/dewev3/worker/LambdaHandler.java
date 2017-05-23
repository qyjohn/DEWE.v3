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

public class LambdaHandler
{
	// Common components
	public AmazonS3Client s3Client;
	public AmazonKinesisClient kinesisClient;
	public String tempDir = "/tmp";
	public String workflow, bucket, prefix, jobId, jobName, command;
	// Logging
	final static Logger logger = Logger.getLogger(LambdaHandler.class);

	/**
	 *
	 * Constructor for Lambda function. 
	 * In this case, the long running job stream is not needed.
	 *
	 */
	 
	public LambdaHandler()
	{
		tempDir = "/tmp/" + UUID.randomUUID().toString();
		s3Client = new AmazonS3Client();
		kinesisClient = new AmazonKinesisClient();
	}

	
	/**
	 *
	 * When the job handler runs on an EC2 instance, it is a function triggered by Lambda.
	 *
	 */

	public void dewev3Handler(KinesisEvent event)
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
			command  = job.attributeValue("command");

			// Download binary and input files
			StringTokenizer st;
			st = new StringTokenizer(job.attribute("binFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				String f = st.nextToken();
				download(1, f);
				runCommand("chmod u+x " + tempDir + "/" + f, tempDir);
			}
			st = new StringTokenizer(job.attribute("inFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				String f = st.nextToken();
				download(2, f);
			}

			// Execute the command and wait for it to complete
			runCommand(command, tempDir);

/*			String env_path = "PATH=$PATH:" + tempDir;
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
*/

			st = new StringTokenizer(job.attribute("outFiles").getValue());
			while (st.hasMoreTokens()) 
			{
				String f = st.nextToken();
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
	 
	public void runCommand(String command, String dir)
	{
		try
		{
			logger.info(command);

			String env_path = "PATH=$PATH:" + dir;
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
			logger.info(result);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}
}
