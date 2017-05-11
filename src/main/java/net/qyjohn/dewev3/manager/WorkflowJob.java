package net.qyjohn.dewev3;

import java.util.HashSet;

public class WorkflowJob
{
	public String jobId, jobName, jobXML, jobCommand;	// job id and job name
	public HashSet<String>	parentJobs, childrenJobs;
	public boolean ready;
	long start_time = 1L;	// default start time, indicating the job has not been started
	int timeout = 1;

	/**
	 *
	 * Constructor
	 *
	 */

	public WorkflowJob(String id, String name, String xml, int t)
	{
		jobId = id;
		jobName = name;
		jobXML = xml;
		jobCommand = name;
		parentJobs = new HashSet<String>();
		childrenJobs = new HashSet<String>();
		timeout = t;
		
		// By default this job is ready to go, unless we find out that it has parents!
		ready = true;
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

	
	/**
	 * 
	 * toString method
	 *
	 */
	 
	public String toString()
	{
		String str = jobId + "\t" + jobName;	// Job ID and name
		if (ready)
		{
			str = str + "\t (ready)";
		}
		else
		{
			str = str + "\t (not ready)";
		}
		
		str = str + "\n    " + jobCommand;

		return str;
	}
	
	
}

