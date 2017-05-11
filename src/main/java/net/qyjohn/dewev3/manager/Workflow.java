package net.qyjohn.dewev3.manager;

import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;

import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class Workflow
{
	public AmazonS3Client client;
	public ConcurrentHashMap<String, WorkflowJob> jobs;
	public String uuid, s3Bucket, s3Prefix;
	public SAXReader reader;
	public Document document;
	public int timeout = 100;

	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public Workflow(String uuid, String bucket, String prefix, int t)
	{
		this.uuid = uuid;
		
		this.s3Bucket = bucket;
		this.s3Prefix = prefix;
		timeout = t;
		client = new AmazonS3Client();
		reader = new SAXReader();
		
		try
		{
			// Initialize the HashMap for workflow jobs
			jobs = new ConcurrentHashMap<String, WorkflowJob>();
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
	 * Parse the work flow from dag.xml.
	 *
	 */
         
	public void parseDocument() throws Exception 
	{
		S3Object s3Object= client.getObject(s3Bucket, s3Prefix+"/dag.xml");
		S3ObjectInputStream in = s3Object.getObjectContent();
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
	 
	public void prepareJob(Element job)
	{
		String id = job.attribute("id").getValue();
		String name = job.attribute("name").getValue();
		job.addAttribute("workflow", uuid);
		job.addAttribute("bucket", s3Bucket);
		job.addAttribute("prefix", s3Prefix);
		
		WorkflowJob wlj = new WorkflowJob(id, name, job.asXML(), timeout);
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
                wlj.addArgument(e.attribute("file").getValue());
            }
            else
            {
	            st = new StringTokenizer(node.getText().trim());
				while (st.hasMoreTokens()) 
				{
					wlj.addArgument(st.nextToken());
				}
            }
		}
	
		jobs.put(id, wlj);
	}

	public static void main(String[] args)
	{
		try
		{
			Workflow wf = new Workflow("Test-UUID-Haha", args[0], args[1], 100);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

