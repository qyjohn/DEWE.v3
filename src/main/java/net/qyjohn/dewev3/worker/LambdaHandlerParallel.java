package net.qyjohn.dewev3.worker;

/**
 *
 * net.qyjohn.dewev3.worker.LambdaHandlerParallel::deweHandler
 *
 * Everything in /tmp/dewe, transient caching, parallel download, parallel execution
 *
 */

public class LambdaHandlerParallel extends LambdaHandlerBase
{
	
 	/**
	 *
	 * Create the execution folder 
	 *
	 */

	public void createEnv()
	{
		runCommand("mkdir -p " + deweDir, "/tmp");
	}

 	/**
	 *
	 * Execute all the jobs
	 *
	 */
	 
	public void executeJobs()
	{
		// Parallel download of binaries using multiple threads
		if (!binFiles.isEmpty())
		{
			try
			{
				Downloader downloader[] = new Downloader[binFiles.size()];
				for (int i=0; i<binFiles.size(); i++)
				{
					downloader[i] = new Downloader("bin", binFiles.get(i), deweDir);
					downloader[i].start();
				}
				for (int i=0; i<binFiles.size(); i++)
				{
					downloader[i].join();
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}		
		
		// Parallel donwload of input files using multiple threads
		if (!inFiles.isEmpty())
		{
			try
			{
				Downloader downloader[] = new Downloader[inFiles.size()];
				for (int i=0; i<inFiles.size(); i++)
				{
					downloader[i] = new Downloader("workdir", inFiles.get(i), deweDir);
					downloader[i].start();
				}
				for (int i=0; i<inFiles.size(); i++)
				{
					downloader[i].join();
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}		

		// Parallel execution of commands using multiple threads
		try
		{
			if (!commands.isEmpty())
			{
				Executor executor[] = new Executor[commands.size()];
				for (int i=0; i<commands.size(); i++)
				{
					executor[i] = new Executor(deweDir + "/" + commands.get(i), deweDir);
					executor[i].start();
				}
				for (int i=0; i<commands.size(); i++)
				{
					executor[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// Parallel upload of output files using multiple threads
		try
		{
			if (!outFiles.isEmpty())
			{
				Uploader uploader[] = new Uploader[outFiles.size()];
				for (int i=0; i<outFiles.size(); i++)
				{
					uploader[i] = new Uploader(outFiles.get(i));
					uploader[i].start();
				}
				for (int i=0; i<outFiles.size(); i++)
				{
					uploader[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		// Parallel ACK of all jobs using multiple threads
		try
		{
			if (!jobs.isEmpty())
			{
				Acker acker[] = new Acker[jobs.size()];
				for (int i=0; i<jobs.size(); i++)
				{
					acker[i] = new Acker(jobs.get(i));
					acker[i].start();
				}
				for (int i=0; i<jobs.size(); i++)
				{
					acker[i].join();
				}
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}
	
	
 	/**
	 *
	 * Clean up execution folder
	 *
	 */

	public void cleanUp()	
	{
		runCommand("rm -Rf " + deweDir, "/tmp");				
	}	
	
	
	class Downloader extends Thread
	{
		String folder, filename, dir;

		public Downloader(String folder, String filename, String dir)
		{
			this.folder = folder;
			this.filename = filename;
			this.dir = dir;
		}

		public void run()
		{
			download_one(folder, filename, dir);
		}
	}
	
	class Uploader extends Thread
	{
		public String filename;

		public Uploader(String filename)
		{
			this.filename = filename;
		}

		public void run()
		{
			upload_one(filename);
		}
	}
	
	class Executor extends Thread
	{
		public String cmd, dir;
		
		public Executor(String cmd, String dir)
		{
			this.cmd = cmd;
			this.dir = dir;
		}
		
		public void run()
		{
			runCommand(cmd, dir);
		}
	}

	class Acker extends Thread
	{
		public String id;
		
		public Acker(String id)
		{
			this.id = id;
		}
		
		public void run()
		{
			sqsClient.sendMessage(ackQueue, id);
		}
	}
}
