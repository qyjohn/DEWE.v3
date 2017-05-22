package net.qyjohn.dewev3.worker;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.spi.v1.*;
import com.google.pubsub.v1.*;
import com.google.protobuf.ByteString;
import com.google.cloud.storage.*;

public class GoogleExecutor extends Thread
{
	// Common components
	public Storage storage;
	public String projectId = ServiceOptions.getDefaultProjectId();
	public TopicName ackTopic;
	public Publisher ackSender;
	public String workflow, bucket, prefix, jobId, jobName, command;
	public String tempDir = "/tmp";
	public ConcurrentHashMap<String, Boolean> cachedFiles;
	Stack<PubsubMessage> jobStack;

	// Logging
	final static Logger logger = Logger.getLogger(GoogleExecutor.class);

	public GoogleExecutor(String workflow, String tempDir, ConcurrentHashMap<String, Boolean> cachedFiles)
	{
		this.workflow	= workflow;
		this.tempDir 	= tempDir;
		this.cachedFiles= cachedFiles;
		
		try
		{
			ackTopic  = TopicName.create(projectId, workflow);
			ackSender = Publisher.defaultBuilder(ackTopic).build();
			storage = StorageOptions.getDefaultInstance().getService();
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}		
	}
	
	public void setJobStack(Stack<PubsubMessage> stack)
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
					PubsubMessage job = jobStack.pop();
					executeJob(job);
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

	public void executeJob(PubsubMessage job)
	{
			try
			{
				workflow = job.getAttributesOrThrow("workflow");
				bucket   = job.getAttributesOrThrow("bucket");
				prefix   = job.getAttributesOrThrow("prefix");
				jobId    = job.getAttributesOrThrow("id");
				
				// Download job definition file 
				download(0, jobId);
				SAXReader reader = new SAXReader();
				Document document = reader.read(new FileInputStream(new File(tempDir + "/" + jobId)));
				Element root = document.getRootElement();
        
//				jobName  = job.getAttributesOrThrow("name");
//				command  = tempDir + "/" + job.getAttributesOrThrow("command");;
				command = root.attribute("command").getValue();
				logger.info(jobId + ":\t" + command);
				

				// Download binary and input files
				StringTokenizer st;
				st = new StringTokenizer(root.attribute("binFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
					download(1, f);
					runCommand("chmod u+x " + tempDir + "/" + f);
				}
				st = new StringTokenizer(root.attribute("inFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
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

				st = new StringTokenizer(root.attribute("outFiles").getValue());
				while (st.hasMoreTokens()) 
				{
					String f = st.nextToken();
					upload(f);
				}

				// Acknowledge the job to be completed
				ackJob(jobId);
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
		if (cachedFiles.get(filename) == null)
		{
			cachedFiles.put(filename, new Boolean(false));
			String key=null, outfile = null;
			if (type==0)	// Job definition
			{
				key = prefix + "/jobs/" + filename;
				outfile = tempDir + "/" + filename;				
			}
			else if (type==1)	// Binary
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
				BlobId blobId = BlobId.of(bucket, key);
				Blob blob = storage.get(blobId);
				if (blob != null)
				{
					InputStream in = new ByteArrayInputStream(blob.getContent()); 
					OutputStream out = new FileOutputStream(outfile);
					IOUtils.copy(in, out);
					in.close();
					out.close();
				} 
			}catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
			cachedFiles.put(filename, new Boolean(true));
		}
	}

	/**
	 *
	 * Upload output data to S3
	 *
	 */
	 
	public void upload(String filename)
	{
		cachedFiles.put(filename, new Boolean(false));
		String key  = prefix + "/workdir/" + filename;
		String file = tempDir + "/" + filename;

		try
		{
			logger.debug("Uploading " + file);
			byte[] data = IOUtils.toByteArray(new FileInputStream(new File(file)));
			Bucket destBucket = storage.get(bucket);
			BlobId blobId = BlobId.of(bucket, key);
			Blob blob = destBucket.create(key, data);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		cachedFiles.put(filename, new Boolean(true));
	}
	
	/**
	 *
	 * ACK to the workflow scheduler that the job is now completed
	 *
	 */
	 
	public void ackJob(String id)
	{
		try
		{
			PubsubMessage message = PubsubMessage.newBuilder()
				.setData(ByteString.copyFrom(id, "UTF-8"))
				.build();
			ackSender.publish(message);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
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
			logger.debug(command);
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
			logger.debug(result);
		} catch (Exception e) 
		{
			logger.error(e.getMessage());
			logger.error(e.getStackTrace());
		}
	}
}
