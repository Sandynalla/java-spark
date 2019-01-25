package com.optum.mgd.spark.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Vector;
import org.apache.commons.io.FileUtils;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SparkUtility {


	public static void deleteDirectory(String directory) throws IOException {
		File dir = new File(directory);
		if(dir.exists()) {
			FileUtils.deleteDirectory(dir);	
			if(dir.exists()) {
				System.out.println("Directory not deleted!");
			}else {
				System.out.println(directory +" directory is deleted!!!");

			}
		}
		else {
			System.out.println(directory + " directory is not existing");
		}
	
	}

	public static String getCsvFile(String fileDirectory) throws FileNotFoundException {
		String fileName = null;
		File path = new File(fileDirectory);

	    File [] files = path.listFiles();
	    for (int i = 0; i < files.length; i++){
	        if (files[i].isFile()){ //this line weeds out other directories/folders
	        	
	            System.out.println(files[i].getName());
	            if(files[i].getName().endsWith(".csv")) {
	            	fileName = files[i].getAbsolutePath();
	            	System.out.println("inside if: "+ fileName);
	            }
	        }
	    }
		return fileName;
	}
	public static void getString() {
		String input = "file:///Users///snalla16///Desktop///UDWExtraxcts///MBRtextFilecsv.txt";
		int lastIndex = input.lastIndexOf("");
		String file = input.substring(0,5);
		String part = input.substring(5,lastIndex);
		System.out.println("file :"+ file);
		System.out.println("part :"+ part);
		String fullPath = file.concat(part);
		System.out.println("fullPath : "+ fullPath);
	
	}

	public final static Session createSesssion(String sftpUSER, String sftpPASS, String sftpHost, int sftpPort)
			throws JSchException {
		Session sftpSession;
		JSch jsch = new JSch();
		sftpSession = jsch.getSession(sftpUSER, sftpHost, sftpPort);
		sftpSession.setPassword(sftpPASS);
		java.util.Properties config = new java.util.Properties();
		config.put("StrictHostKeyChecking", "no");
		sftpSession.setConfig(config);
		sftpSession.connect();

		return sftpSession;
	}

	public final static Channel createSFTPChannel(Session sftpSession) throws JSchException {
		Channel channel = sftpSession.openChannel("sftp");
		channel.connect();
		return channel;
	}


	public static String getFileNamesFromECG(String fileNameRegx) throws IOException, JSchException, SftpException {

		String fileName = null;
		String regxMatchedFileName = null;
		Session session = null;
		Channel channel = null;
		ChannelSftp channelSftp = null;
		LoadProperties properties = new LoadProperties();
		 
		try {
			System.out.println("Connecting to ECG");
			session = SparkUtility.createSesssion(properties.getSftpUser(), properties.getSftpPass(), properties.getSftpHost(), 22);
			channel = SparkUtility.createSFTPChannel(session);
			channelSftp = (ChannelSftp) channel;
			channelSftp.cd("aceudw/dev");
			Vector<LsEntry> entries = channelSftp.ls("*.*");

			for (LsEntry entry : entries) {
				fileName = entry.getFilename();
				if (fileName.endsWith(".gz") && fileName.matches(fileNameRegx)) {
				  
				    	
					regxMatchedFileName = fileName;
					System.out.println("regx on ECG : "+regxMatchedFileName);

				}
			}
		} finally {

			if (channel != null && channel.isConnected()) {
				channel.disconnect();
				System.out.println("Connection cloded on ECG");

			}

		}
		return regxMatchedFileName;
	}
	

	public static String getFileSizeinGB(String fileName) {

		File file = new File(fileName);
		double fileSizeInKB = (file.length() / 1024);
		double fileSizeInMB = (fileSizeInKB / 1024);
		double fileSizeInGB = (fileSizeInMB / 1024);
		System.out.println("Exiting getFileSizeinMB method with fileName = " + fileName +" of Size "+fileSizeInGB +" G");
		return Double.toString(fileSizeInGB)+" G";
	}
	public static void main(String args[]) throws IOException, JSchException, SftpException {
		SparkUtility util = new SparkUtility();
		System.out.println("From main");
		String fileName = util.getFileNamesFromECG("MBR.0001.Member.[0-9].*gz");
		System.out.println("File name on ECG :"+ fileName);
	}
		
}
