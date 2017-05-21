package net.qyjohn.dewev3.manager;


public class WorkflowTransceiver
{
	/*
	 *
	 * Delete the temporary topic / queue / stream when the workflow is completed.
	 *
	 */
	
	public void cleanUp()
	{
 	}
	
	
	
	/**
	 *
	 * Publishing a job to the corresponding topic / queue / stream.
	 *
	 */
	 
	public void publishJob(WorkflowJob job)
	{
	}
	
	/**
	 *
	 * Receive job completion information from the ACK topic / queue / stream.
	 *
	 */

	public String[] receiveAck()
	{
		return null;
	}
}

