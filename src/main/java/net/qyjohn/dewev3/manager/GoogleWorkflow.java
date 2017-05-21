package net.qyjohn.dewev3.manager;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import org.dom4j.*;
import org.dom4j.io.SAXReader;

import com.google.cloud.storage.*;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;


public class GoogleWorkflow
{
	Storage storage;
	public ConcurrentHashMap<String, WorkflowJob> jobs;
//	public HashMap<String, WorkflowJob> jobs;
	public String uuid, bucket, prefix;
	public SAXReader reader;
	public Document document;
	List<String> longJobs = new ArrayList<String>();
	final static Logger logger = Logger.getLogger(GoogleWorkflow.class);
	public boolean localExec = false;

	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public GoogleWorkflow(String uuid, String bucket, String prefix, boolean localExec)
	{
		this.uuid = uuid;
		this.localExec = localExec;
		
		this.bucket = bucket;
		this.prefix = prefix;
		storage = StorageOptions.getDefaultInstance().getService();
		reader = new SAXReader();
		
		try
		{
			// Initialize the HashMap for workflow jobs
			checkLongJobs();
			jobs = new ConcurrentHashMap<String, WorkflowJob>();
//			jobs = new HashMap<String, WorkflowJob>();
			parseDocument();
			parseWorkflow();	
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
	}
	
	public boolean isEmpty()
	{
		return jobs.isEmpty();
	}
	
	
	
	/**
	 *
	 * Load long-running job names from long.xml.
	 *
	 */
         
	public void checkLongJobs() throws Exception 
	{
		BlobId blobId = BlobId.of(bucket, prefix+"/long.xml");
		Blob blob = storage.get(blobId);
		if (blob != null)
		{
			String  jobXML = new String(blob.getContent());
			Element jobs = DocumentHelper.parseText(jobXML).getRootElement();

			for ( Iterator iter = jobs.elementIterator( "job" ); iter.hasNext(); ) 
			{
				Element job = (Element) iter.next();
				longJobs.add(job.attribute("name").getValue());
			}
			logger.info("Found long-running job definition long.xml with the following jobs:");
			for (String s : longJobs)
			{
				logger.info("\t" + s);
			}
		}
		else
		{
			logger.info("The workflow does not long-running job definition long.xml.");
		}

		if (localExec)
		{
			logger.info("Workflow scheduler enforces local execution. All jobs are treated as long-running jobs.");
		}

	}

	
	
	/**
	 *
	 * Parse the work flow from dag.xml.
	 *
	 */
         
	public void parseDocument() throws Exception 
	{
		BlobId blobId = BlobId.of(bucket, prefix+"/dag.xml");
		Blob blob = storage.get(blobId);
		ByteArrayInputStream in = new ByteArrayInputStream(blob.getContent());
		document = reader.read(in);
	}
	
	/**
	 *
	 * Parse jobs and job dependencies
	 *
	 */
	 
	public void parseWorkflow()
	{
		List<Element> jobs = document.getRootElement().elements("job");
		List<Element> children = document.getRootElement().elements("child");

		for(Element job : jobs) 
		{
			prepareJob(job);
		}
		for(Element child : children) 
		{
			prepareChild(child);
		}
		jobs = null;
		children = null;
	}
	
	
	/**
	 *
	 * Parse the dependencies of a job
	 *
	 */
	 
	public void prepareChild(Element child)
	{
		String child_id = child.attribute("ref").getValue();
		List<Element> parents = child.elements("parent");
		
		for (Element parent: parents)
		{
			String parent_id = parent.attribute("ref").getValue();
			jobs.get(child_id).addParent(parent_id);
			jobs.get(parent_id).addChild(child_id);
		}
	}

	
	/**
	 *
	 * Parse a job, extract job name (command) and command line arguments
	 *
	 */
	 
	public void prepareJob(Element element)
	{
		String id = element.attribute("id").getValue();
		String name = element.attribute("name").getValue();
		String command = name;		
		
		// Compose the command to execute
		Element args = element.element("argument");
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
		
		// Improve the XML representation of the job
		element.addAttribute("workflow", uuid);
		element.addAttribute("bucket", bucket);
		element.addAttribute("prefix", prefix);
		element.addAttribute("command", command);		
		
		// Create a WorkflowJob object
//		WorkflowJob job = new WorkflowJob(id, name, element.asXML());	
		WorkflowJob job = new WorkflowJob(uuid, bucket, prefix, id, element);

		job.setCommand(command);
		job.setLongJob(localExec);
		if (longJobs.contains(name))
		{
			job.setLongJob(true);
		}
		jobs.put(id, job);
		logger.info(id + "\t" + command);
	}
}

