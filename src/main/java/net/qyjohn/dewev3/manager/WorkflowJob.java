package net.qyjohn.dewev3.manager;

import java.util.*;
import org.dom4j.*;
import org.dom4j.io.SAXReader;
import org.apache.log4j.Logger;


public class WorkflowJob
{
	public String workflow, bucket, prefix;
	public String jobId, jobName, jobXML, jobCommand;	// job id and job name
	HashSet<String> parentJobs 		= new HashSet<String>();
	HashSet<String> childrenJobs 	= new HashSet<String>();
	HashSet<String> binFiles 		= new HashSet<String>();
	HashSet<String> inFiles  		= new HashSet<String>();
	HashSet<String> outFiles 		= new HashSet<String>();
	public boolean ready;
	public boolean isLongJob = false;
	final static Logger logger = Logger.getLogger(WorkflowJob.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public WorkflowJob(String id, String name, String xml)
	{
		jobId = id;
		jobName = name;
		jobXML = xml;
		jobCommand = name;
		
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;		
		logger.debug(jobXML);
	}

	public WorkflowJob(String workflow, String bucket, String prefix, String id, String name, String xml)
	{
		this.workflow = workflow;
		this.bucket = bucket;
		this.prefix = prefix;
		this.jobId = id;
		this.jobName = name;
		this.jobXML = xml;
		
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;		
		logger.debug(jobXML);
	}
	
	public WorkflowJob(String workflow, String bucket, String prefix, String id, Element e)
	{
		// UUID of workflow, S3/GS bucket and prefix
		this.workflow = workflow;
		this.bucket = bucket;
		this.prefix = prefix;
		
		// Job id, name, initial command	
		jobId = id;
		jobName = e.attribute("name").getValue();
		jobCommand = jobName;
		
		// Command with full parameters
		Element args = e.element("argument");
		StringTokenizer st;
		for ( int i = 0, size = args.nodeCount(); i < size; i++ )
		{
			Node node = args.node(i);
			if ( node instanceof Element ) 
			{
				Element e1 = (Element) node;
				jobCommand = jobCommand + " " + e1.attribute("name").getValue();
			}
			else
			{
				st = new StringTokenizer(node.getText().trim());
				while (st.hasMoreTokens()) 
				{
					jobCommand = jobCommand + " " + st.nextToken();
				}
			}
		}
		
		// Parse binary, input and output files
		binFiles.add(jobName);
		parseFiles(e);

		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;
	}
	
	public void setLongJob(boolean type)
	{
		isLongJob = type;	
	}
	

	public void parseFiles(Element e)
	{
				for ( Iterator iter = e.elementIterator( "uses" ); iter.hasNext(); ) 
				{
					Element file = (Element) iter.next();
					if (file.attribute("link").getValue().equals("input"))
					{
						// This is an input file
						if (file.attribute("executable") != null)
						{
							if (file.attribute("executable").getValue().equals("true"))
							{
								binFiles.add(file.attribute("name").getValue());							
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
	}	

	
	public void setCommand(String cmd)
	{
		jobCommand = cmd;
	}
	
	 
	/**
	 *
	 * Add a child to the job
	 *
	 */

	public void addChild(String child_id)
	{
		childrenJobs.add(child_id);
	}

	/**
	 *
	 * Add a parent to the job
	 *
	 */

	public void addParent(String parent_id)
	{
		parentJobs.add(parent_id);
		ready = false;
	}
	
	/**
	 *
	 * Remove a parent from the job
	 *
	 */

	public void removeParent(String parent_id)
	{
		parentJobs.remove(parent_id);
		if (parentJobs.isEmpty())
		{
			ready = true;
		}
	}	
}

