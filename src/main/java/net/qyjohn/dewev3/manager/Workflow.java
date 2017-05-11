package net.qyjohn.dewev3;

import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Iterator;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.Node;
import org.dom4j.DocumentException;
import org.dom4j.io.SAXReader;


//import com.amazonaws.*;
//import com.amazonaws.auth.*;
//import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class Workflow
{
        public AmazonS3Client client;
	public ConcurrentHashMap<String, WorkflowJob> initialJobs, queueJobs, runningJobs;
	public String s3Bucket, s3Prefix;
	public SAXReader reader;
	public Document document;
	public int timeout = 100;

	
	/**
	 *
	 * Constructor
	 *
	 */
	 
	public Workflow(String bucket, String prefix)
	{
		this.s3Bucket = bucket;
		this.s3Prefix = prefix;
                client = new AmazonS3Client();
		reader = new SAXReader();
		
		try
		{
			// Initialize the HashMap for workflow jobs
			initialJobs = new ConcurrentHashMap<String, WorkflowJob>();
			queueJobs = new ConcurrentHashMap<String, WorkflowJob>();
			runningJobs = new ConcurrentHashMap<String, WorkflowJob>();
			parseDocument();
			parseWorkflow();	
		} catch (Exception e)
		{
			System.out.println(e.getMessage());	
			e.printStackTrace();
		}
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
			initialJobs.get(child_id).addParent(parent_id);
			initialJobs.get(parent_id).addChild(child_id);
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
		
		System.out.println("\n\n" + id + "\t" + name);
		System.out.println(job.asXML());

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
	
		initialJobs.put(id, wlj);
	}

	public static void main(String[] args)
	{
		try
		{
			Workflow wf = new Workflow(args[0], args[1]);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
}

