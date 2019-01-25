package com.optum.spark.java.entity;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.sql.Date;

public class MemberC implements Serializable{
	
	public MemberC() {
		
	}
	
	private int MBR_PTY_ID;
	//private String MBR_PTY_ID;
	private String GIV_NM;
	private String FAM_NM;
	private String SRC_SBSCR_ID;
	private int DEPN_NBR;
	//private String DEPN_NBR;
	private String DEPN_SEQ_NUM;
	private String REL_CD;
	private Date BTH_DT;
	//private String BTH_DT;
	private String GDR_TYP_CD;
	private Date MBR_ROW_EFF_DT;
	private Date MBR_ROW_EXPIR_DT;
/*	private String MBR_ROW_EFF_DT;
	private String MBR_ROW_EXPIR_DT;*/
	
	 public MemberC(String [] args) throws ParseException{
	        //this.MBR_PTY_ID =(args[0]);
	        this.MBR_PTY_ID =Integer.parseInt(args[0]);
	        
	        this.GIV_NM =args[1];
	        this.FAM_NM = args[2];
	        this.SRC_SBSCR_ID = args[3];
	        this.DEPN_NBR = Integer.parseInt(args[4]);
	        this.DEPN_SEQ_NUM = args[5];
	        this.REL_CD = args[6];
	        this.BTH_DT = (Date) new SimpleDateFormat("yyyyMMdd").parse(args[7]);  
	        //this.BTH_DT = args[7];
	        this.GDR_TYP_CD = args[8];
	        this.MBR_ROW_EFF_DT = (Date) new SimpleDateFormat("yyyyMMdd").parse(args[9]); 
	        this.MBR_ROW_EXPIR_DT = (Date) new SimpleDateFormat("yyyyMMdd").parse(args[10]); 
	      /*  this.MBR_ROW_EFF_DT = (args[9]); 
	        this.MBR_ROW_EXPIR_DT = (args[10]); */
	    }
	 /*	 public MemberC(Integer mbrPtyId, String givNm, String famNm, String srcSbscrId, Integer depnDbr,String depnSeqnum, String relCD, Date bthDt, String gdrTypCd, Date mbrRowEffDt, Date mbrRowExpiryDt ) throws ParseException{
	        this.MBR_PTY_ID =mbrPtyId;
	        this.GIV_NM =givNm;
	        this.FAM_NM = famNm;
	        this.SRC_SBSCR_ID = srcSbscrId;
	        this.DEPN_NBR = depnDbr;
	        this.DEPN_SEQ_NUM = depnSeqnum;
	        this.REL_CD =relCD;
	        this.BTH_DT = bthDt;  
	        this.GDR_TYP_CD = gdrTypCd;
	        this.MBR_ROW_EFF_DT = mbrRowEffDt; 
	        this.MBR_ROW_EXPIR_DT = mbrRowExpiryDt; 
	        
	    }*/
	public int getMBR_PTY_ID() {
		return MBR_PTY_ID;
	}
	public void setMBR_PTY_ID(int mBR_PTY_ID) {
		MBR_PTY_ID = mBR_PTY_ID;
	}
	public String getGIV_NM() {
		return GIV_NM;
	}
	public void setGIV_NM(String gIV_NM) {
		GIV_NM = gIV_NM;
	}
	public String getFAM_NM() {
		return FAM_NM;
	}
	public void setFAM_NM(String fAM_NM) {
		FAM_NM = fAM_NM;
	}
	public String getSRC_SBSCR_ID() {
		return SRC_SBSCR_ID;
	}
	public void setSRC_SBSCR_ID(String sRC_SBSCR_ID) {
		SRC_SBSCR_ID = sRC_SBSCR_ID;
	}
	public int getDEPN_NBR() {
		return DEPN_NBR;
	}
	public void setDEPN_NBR(int dEPN_NBR) {
		DEPN_NBR = dEPN_NBR;
	}
	public String getDEPN_SEQ_NUM() {
		return DEPN_SEQ_NUM;
	}
	public void setDEPN_SEQ_NUM(String dEPN_SEQ_NUM) {
		DEPN_SEQ_NUM = dEPN_SEQ_NUM;
	}
	public String getREL_CD() {
		return REL_CD;
	}
	public void setREL_CD(String rEL_CD) {
		REL_CD = rEL_CD;
	}
	public Date getBTH_DT() {
		return BTH_DT;
	}
	public void setBTH_DT(Date bTH_DT) {
		BTH_DT = bTH_DT;
	}
	public String getGDR_TYP_CD() {
		return GDR_TYP_CD;
	}
	public void setGDR_TYP_CD(String gDR_TYP_CD) {
		GDR_TYP_CD = gDR_TYP_CD;
	}
	public Date getMBR_ROW_EFF_DT() {
		return MBR_ROW_EFF_DT;
	}
	public void setMBR_ROW_EFF_DT(Date mBR_ROW_EFF_DT) {
		MBR_ROW_EFF_DT = mBR_ROW_EFF_DT;
	}
	public Date getMBR_ROW_EXPIR_DT() {
		return MBR_ROW_EXPIR_DT;
	}
	public void setMBR_ROW_EXPIR_DT(Date mBR_ROW_EXPIR_DT) {
		MBR_ROW_EXPIR_DT = mBR_ROW_EXPIR_DT;
	}
/*	public String getMBR_PTY_ID() {
		return MBR_PTY_ID;
	}
	public void setMBR_PTY_ID(String mBR_PTY_ID) {
		MBR_PTY_ID = mBR_PTY_ID;
	}*/
/*	public String getDEPN_NBR() {
		return DEPN_NBR;
	}
	public void setDEPN_NBR(String dEPN_NBR) {
		DEPN_NBR = dEPN_NBR;
	}*/
	/*public void setBTH_DT(String bTH_DT) {
		BTH_DT = bTH_DT;
	}
	public void setMBR_ROW_EFF_DT(String mBR_ROW_EFF_DT) {
		MBR_ROW_EFF_DT = mBR_ROW_EFF_DT;
	}
	public void setMBR_ROW_EXPIR_DT(String mBR_ROW_EXPIR_DT) {
		MBR_ROW_EXPIR_DT = mBR_ROW_EXPIR_DT;
	}
*/
}
