package com.dm.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



public class FileUtil {

	
	public static boolean createDir(String destPath) 
	{
		//Creation of new Directory

	  // Configuration conf = new Configuration ();
	   // conf.set("fs.defaultFS", "hdfs://localhost:8020");
	    //FileSystem fileSystem = FileSystem.get(conf);
	    boolean isDirCreated = false;
	    File fileSytem = new File(destPath);
	    try {
	    	if(!fileSytem.exists())
	    	isDirCreated= fileSytem.mkdirs() ;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	    return isDirCreated;

	}
	
	public static boolean moveFiles(File aSourceDir, File aDestinationDir) {
		boolean isFilesMoved = false;
		if (aSourceDir.isDirectory()) {
			File[] aListFiles = aSourceDir.listFiles();
			int aSourceFilesCnt = aListFiles.length;
			int aMoveFilesCnt = 0;
			for (File aFile : aListFiles) {
				boolean isFileMoved = moveFile(aFile, aDestinationDir);
				if (isFileMoved) {
					aMoveFilesCnt++;
					System.out.println("Success fully moved: " + aFile);

				} else {
					System.out.println("File moved unsuccess full: " + aFile);
				}
			}

			if (aSourceFilesCnt == aMoveFilesCnt) {
				isFilesMoved = true;
				System.out.println("All files moved successfully.");
			} else if (aMoveFilesCnt > 0 && aMoveFilesCnt < aSourceFilesCnt) {
				System.out.println("Files moved partially.");
			} else {
				System.out.println("Files are not moved.");
			}
		}
		return isFilesMoved;
	}
	
	public static boolean moveFile(File sourceFile, File destinationPath) {
		return sourceFile.renameTo(new File(destinationPath, sourceFile
				.getName()));
	}
	/*public static boolean copyLocalFileToHDFS(final String input,final String hdfsOutputDir, final Configuration conf) throws IOException
	{
		
		System.out.println("Congrats!!!In copyLocalFileToHDFS Method");
		
		
		final Path hdfsPath=new Path(hdfsOutputDir+"/"+input);
		
		
		final InputStream is=new FileInputStream(input);
		
		FileSystem hdfs=FileSystem.get(conf);
		
		FSDataOutputStream out=hdfs.create(hdfsPath);
		
		
		IOUtils.copyBytes(is, out, conf);
		
		
		return Boolean.TRUE;

  	}*/
/*public static boolean copyLocalDirToHDFS(final String input,final String hdfsOutputDir, final Configuration conf) throws IOException
	{
		
		System.out.println("Congrats!!!In copyLocalDirToHDFS Method");
		
		
		final Path hdfsPath=new Path(hdfsOutputDir+"/"+input);
		
		
		final InputStream is=new FileInputStream(input);
		
		FileSystem hdfs=FileSystem.get(conf);
		
		FSDataOutputStream out=hdfs.create(hdfsPath);
		
		
		IOUtils.copyBytes(is, out, conf);
		
		
		return Boolean.TRUE;

	}
*/
	public static boolean copyLocalFileToHDFS(
		org.apache.hadoop.conf.Configuration conf, FileSystem hdfs,
		String input, String hdfsOutputDir) {
		
	System.out.println("Congrats!!!In copyLocalDirToHDFS Method");
	
	String inputFileName = getFileName(input);
	
	final Path hdfsPath=new Path(hdfsOutputDir+separator+inputFileName);
	
	
	
	
	FSDataOutputStream out;
	try {
		final InputStream is=new FileInputStream(input);
		out = hdfs.create(hdfsPath);
		IOUtils.copyBytes(is, out, conf);
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	
	
	return false;
	
	
}
public static String separator = System.getProperty("file.separator");
public static String getFileName(String inputFile){
	String inputFileName= inputFile;
	
	int idx = StringUtils.lastIndexOf(inputFileName, separator);
	if(idx != -1){
		inputFileName = StringUtils.substring(inputFile, idx);
	}
	
	return inputFileName;
}

/*public static boolean deleteFile(Path path) {
	return path.delete();
}

public boolean deleteDirectory(Path aDirectory) {

	boolean isDirDeleted = false;
	if (aDirectory.isDirectory()) {
		if (aDirectory.listFiles().length == 0) {
			if (!(aDirectory.getAbsolutePath())
					.equalsIgnoreCase(LSEnginePropertiesReader
							.getInstance().getProperty(
									LSEngineProperties.BASE_LOCATION)))
				isDirDeleted = aDirectory.delete();
			System.out.println("Deleted: " + aDirectory.getAbsolutePath());
			deleteDirectory(aDirectory.getParentFile());
		}
	}
	return isDirDeleted;
}*/
}