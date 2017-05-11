package net.qyjohn.dewev3;

import java.io.*;
import java.util.*;
import org.apache.commons.io.IOUtils;
import com.amazonaws.services.lambda.runtime.*; 
import com.amazonaws.services.lambda.runtime.events.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class LambdaWorker
{

	public AmazonS3Client client;

	public LambdaWorker()
	{
		client = new AmazonS3Client();
	}
	
	public void jobHandler(KinesisEvent event)
	{
		for(KinesisEvent.KinesisEventRecord rec : event.getRecords())
		{
			try
			{
				String job = new String(rec.getKinesis().getData().array());
				System.out.println(job);
				// Extract job information
				// Send the job id with status to the ACK Kinesis stream

				// ACK that the job is now running

				// Actually run the job

				// ACK that the job is now complete
				// Send the job id with status to the ACK Kinesis stream
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
