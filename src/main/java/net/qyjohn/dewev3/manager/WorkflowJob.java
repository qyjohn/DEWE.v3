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
	public boolean ready;
	public boolean isLongJob = false;
	final static Logger logger = Logger.getLogger(WorkflowJob.class);

	/**
	 *
	 * Constructor
	 *
	 */

	public WorkflowJob(String workflow, String bucket, String prefix, String id, String name, String xml)
	{
		this.workflow = workflow;
		this.bucket   = bucket;
		this.prefix   = prefix;
		this.jobId    = id;
		this.jobName  = name;
		this.jobXML   = xml;
		
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;		
		logger.debug(jobXML);
	}

	
	public void setLongJob(boolean type)
	{
		isLongJob = type;	
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

