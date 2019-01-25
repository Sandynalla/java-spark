package com.optum.mgd.spark.util;

import java.util.Properties;

public class LoadProperties {

	public LoadProperties(){
		getProperties();	
	
	}
	private String dbUrl;
	private String dbUser;
	private String dbPass;
	private String sftpHost;
	private String sftpUser;
	private String sftpPass;

	protected void getProperties() {
		
		Properties properties = null;
        properties = new Properties();
        try {
            properties.load(HibernateUtil.class.getClassLoader()
                    .getResourceAsStream("dbconnections.properties"));
             } catch (Exception e) {
            e.printStackTrace();
        }
       
	 this.dbUrl = properties.getProperty("hibernate.connection.url");
	 this.dbPass = properties.getProperty("hibernate.connection.password");
	 this.dbUser =  properties.getProperty("hibernate.connection.username");
	 this.sftpHost= properties.getProperty("SFTP.HOST");
	 this.sftpPass=properties.getProperty("SFTP.PASS");
	 this.sftpUser=properties.getProperty("SFTP.USER");
      
	}
	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	public String getDbPass() {
		return dbPass;
	}

	public void setDbPass(String dbPass) {
		this.dbPass = dbPass;
	}

	public String getSftpHost() {
		return sftpHost;
	}

	public void setSftpHost(String sftpHost) {
		this.sftpHost = sftpHost;
	}

	public String getSftpUser() {
		return sftpUser;
	}

	public void setSftpUser(String sftpUser) {
		this.sftpUser = sftpUser;
	}

	public String getSftpPass() {
		return sftpPass;
	}

	public void setSftpPass(String sftpPass) {
		this.sftpPass = sftpPass;
	}
	
}
