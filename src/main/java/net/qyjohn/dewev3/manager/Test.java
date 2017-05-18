package net.qyjohn.dewev3.manager;

import java.io.*;
import java.nio.*;
import java.util.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.kinesis.*;
import com.amazonaws.services.kinesis.model.*;
import net.qyjohn.dewev3.worker.*;
import org.apache.log4j.Logger;


public class Test
{

	public AmazonKinesisClient client;
	public String jobStream;
	public String jobXML;
	
	public Test()
	{
		try
		{
			// The Kinesis stream to publish jobs
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			jobStream = prop.getProperty("jobStream");
			client = new AmazonKinesisClient();
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();				
		}
	}
	
	public void readTestJob()
	{
		try
		{
			InputStream is = new FileInputStream("testjob.xml"); 
			BufferedReader buf = new BufferedReader(new InputStreamReader(is)); 
			String line = buf.readLine(); 
			StringBuilder sb = new StringBuilder(); 
			while(line != null)
			{ 
				sb.append(line).append("\n"); 
				line = buf.readLine(); 
			} 
			jobXML = sb.toString();
			
			System.out.println(jobXML);
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();	
		}		
	}
	
	public void sendTestData()
	{
		byte[] bytes = jobXML.getBytes();
		PutRecordRequest putRecord = new PutRecordRequest();
		putRecord.setStreamName(jobStream);
		putRecord.setPartitionKey(UUID.randomUUID().toString());
		putRecord.setData(ByteBuffer.wrap(bytes));

		try 
		{
			client.putRecord(putRecord);
		} catch (Exception e) 
		{
			System.out.println(e.getMessage());
			e.printStackTrace();	
		}
	}
	
	public static void main(String[] args)
	{
		Test test = new Test();
		test.readTestJob();
		test.sendTestData();
	}

}