package com.optum.spark.java.entity;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

@Entity
@Table(name="LOADHISTORY")
public class LoadHistory implements Serializable {

	private static final long serialVersionUID = 1L;
	
	public LoadHistory() {
		super();
	}
	@GeneratedValue(strategy = GenerationType.SEQUENCE,generator = "LOADHISTORY_SEQ")
    @SequenceGenerator(name = "LOADHISTORY_SEQ", sequenceName = "LOADHISTORY_SEQ",allocationSize=1)
    @Id
    @Column(name = "LOADHISTORYID")
	private Integer loadHistoryID;
	
	@Column(name = "TABLENAME")
	private String tableName;
    
	@Column(name = "LOADSTART")
	private Date loadStart;
    
    @Column(name = "LOADFINISH")
	private Date loadFinish;
    
    @Column(name = "STATUSMESSAGE")
  	private String statusMessage;
  
    @Column(name = "SUCCESS")
   	private String success;
    
    @Column(name = "FILENAME")
	private String fileName;
    
    @Column(name = "FILESIZE")
	private String fileSize;

	public Integer getLoadHistoryID() {
		return loadHistoryID;
	}

	public void setLoadHistoryID(Integer loadHistoryID) {
		this.loadHistoryID = loadHistoryID;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Date getLoadStart() {
		return loadStart;
	}

	public void setLoadStart(Date loadStart) {
		this.loadStart = loadStart;
	}

	public Date getLoadFinish() {
		return loadFinish;
	}

	public void setLoadFinish(Date loadFinish) {
		this.loadFinish = loadFinish;
	}

	public String getStatusMessage() {
		return statusMessage;
	}

	public void setStatusMessage(String statusMessage) {
		this.statusMessage = statusMessage;
	}

	public String getSuccess() {
		return success;
	}

	public void setSuccess(String success) {
		this.success = success;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getFileSize() {
		return fileSize;
	}

	public void setFileSize(String fileSize) {
		this.fileSize = fileSize;
	}
    
	
	

}

