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
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}


	public void mergeHandler(KinesisEvent event)
	{
	}
}
