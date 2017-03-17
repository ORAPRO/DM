package com.dm.util;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.dm.PropertyReader;


public class CronJob extends Configured implements Tool {

		//private static DMProperties DMProperties;

	static PropertyReader propertyReader = null;
	public static void main(String[] args) {
		System.out.println("In MAIN Method");

		// Step-1 validate input arguments
		if (args.length < 1) {
			System.out.println("Java Usage "
							+ CronJob.class.getName()
							+ "In valid arguments lenth. Data Migration properties file path is required.");
			return;
		}
		
		propertyReader = new PropertyReader();
		//propertyReader = PropertyReader.getPropertyReader();
		propertyReader.loadProperties(args[0]);

	// Step-2 Initialize configuration
	Configuration Conf = new Configuration(Boolean.TRUE);
	Conf.set("fs.defaultFS", "hdfs://localhost:8020");

	// Step-3 Run ToolRunner.run method to set the arguments to config.
	try {
		int i = ToolRunner.run(Conf, new CronJob(), args);
		if (i == 0) {
			System.out.println(HDFSUtils.SUCCESS);
		} else {
			System.out.println(HDFSUtils.FAILED + " STATUS Code: " + i);
		}
	} catch (Exception e) {
		System.out.println(HDFSUtils.FAILED);
		e.printStackTrace();
	}
}

@Override
public int run(String[] paramArrayOfString) throws Exception {
	System.out.println("In Run Method");

	// Local System
	final String tBaseLocation = propertyReader.getProperty(DMProperties.BASE_LOCATION);
	final String tFileSourceLocation = tBaseLocation+ HDFSUtils.FILE_SEPARATOR+ propertyReader
					.getProperty(DMProperties.LANDING_ZONE);
	FileUtil.createDir(tFileSourceLocation);
	final String tArchiveLocation = tBaseLocation+ HDFSUtils.FILE_SEPARATOR+ propertyReader
			.getProperty(DMProperties.ARCHIVE);
	FileUtil.createDir(tArchiveLocation);
	final String tFailedLocation = tBaseLocation+ HDFSUtils.FILE_SEPARATOR+ propertyReader
			.getProperty(DMProperties.FAILED);
	FileUtil.createDir(tFailedLocation);
	
	
		// Load the configuration
		Configuration Conf = getConf();
		// Create a instance for File System object.
		FileSystem hdfs = FileSystem.get(Conf);
		
		// HDFS File system
		final String tHDFSBaseLocation = propertyReader.getProperty(DMProperties.HDFS_BASE_LOCATION);
		final String tDestinationPath = tHDFSBaseLocation+ HDFSUtils.FILE_SEPARATOR
				+ propertyReader.getProperty(DMProperties.HDFS_LANDING_ZONE);

		// Create directory on HDFS File System if does not exist.
		HDFSUtils.createHDFSDirectories(hdfs, tDestinationPath);

		boolean cronProcess = Boolean.TRUE;
		while (cronProcess) {
			File tInboxDir = new File(tFileSourceLocation);

			if (tInboxDir.isDirectory()) {
				File[] tListFiles = tInboxDir.listFiles();
				for (File tInputFile : tListFiles) {
					String[] args = { tInputFile.getAbsolutePath().toString(),
							tDestinationPath };

					boolean isCopied = HDFSUtils
							.copyFromLocal(Conf, hdfs, args);
					if (isCopied) {
						FileUtil.moveFile(tInputFile, new File(tArchiveLocation));
					} else {
						FileUtil.moveFile(tInputFile, new File(tFailedLocation));
					}
				}
			}
			
			// Frequency
			//            millsec * sec * min
			Thread.sleep(1000 * 60 * 2);
		}
		return 0;
	}
}
