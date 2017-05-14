package net.qyjohn.dewev3.manager;

import java.util.HashSet;

public class WorkflowJob
{
	public String jobId, jobName, jobXML, jobCommand;	// job id and job name
	public HashSet<String>	parentJobs, childrenJobs;
	public boolean ready;
	public boolean isLongJob = false;

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
		parentJobs = new HashSet<String>();
		childrenJobs = new HashSet<String>();
		
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;
	}
	
	public void setLongJob(boolean type)
	{
		isLongJob = type;	
	}
	
	/**
	 *
	 * Add arguments to the job
	 *
	 */
	
	public void addArgument(String args)
	{
		jobCommand = jobCommand + " " + args;
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

