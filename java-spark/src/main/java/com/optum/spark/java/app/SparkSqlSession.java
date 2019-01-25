package com.optum.spark.java.app;

import java.sql.SQLException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import com.optum.mgd.spark.dao.impl.DBUtility;
import com.optum.mgd.spark.util.LoadProperties;
import com.optum.mgd.spark.util.SparkUtility;

public class SparkSqlSession {

	private static SparkSession sparkSession;
	private static final String MEMBER_A = "";
	private static final String MEMBER_B = "";
	private static final String MEMBERALTERNATE_A = "";
	private static final String MEMBERALTERNATE_B = "";
	private static final String MEMBERCOVERAGE_A = "";
	private static final String MEMBERCOVERAGE_B = "";
	private static final String csvMemberDirectory = "";
	private static final String csvMemberAtlDirectory = "";
	private static final String csvMemberCovDirectory = "";
	private static final String memberGzFile = "";
	private static final String memberAltIdGzFile = "";
	private static final String memberCoverageGzFile = "";
	private static final DBUtility dbUtility = new DBUtility();
	private static final LoadProperties properties = new LoadProperties();

	private static String dbUser;
	private static String dbPass;
	private static String dbUrl;
	static {
		dbUser=properties.getDbUser();
		dbPass=properties.getDbPass();
		dbUrl=properties.getDbUrl();
		
	}
	/*
	 * public static void main(String args[]) throws InterruptedException {
	 * Callable<Object> memberCall = Executors.callable(new LaadMember());
	 * Callable<Object> memberAltIdCall = Executors.callable(new LoadMemberAltId());
	 * Callable<Object> memberCovCall = Executors.callable(new
	 * LaadMemberCoverage()); Set<Callable<Object>> calls = new HashSet<>();
	 * calls.add(memberCall); calls.add(memberAltIdCall);
	 * //calls.add(memberCovCall);
	 * 
	 * //ExecutorService service = Executors.newCachedThreadPool(); ExecutorService
	 * service = Executors.newFixedThreadPool(3);
	 * 
	 * service.invokeAll(calls); service.shutdown(); System.exit(0);
	 * 
	 * }
	 */
	public static class LaadMember implements Runnable {

		static String membTableToLoad = null;
		static String membTableView = null;
		static Date membLoadStart = null;
		static Date membFinishTime = null;
		static String membCsvFile = null;

		@Override
		public void run() {
			sparkSession = new SparkSession.Builder().master("local[*]").appName("Java JDBC Spark").getOrCreate();
			System.out.println("SparkSession for Member is created.");
			try {
				laodMemberFile(sparkSession);
			} catch (SparkException | AnalysisException | FileNotFoundException | InterruptedException e) {

				System.out.println("Exception occured in memberAltLoad :" + e);
				// send email notification
				e.printStackTrace();
			} finally {
				try {
					SparkUtility.deleteDirectory(csvMemberDirectory);
				} catch (IOException e) {
					e.printStackTrace();
					System.out.println("exception occured while deleting the Directory : " + csvMemberDirectory);

				}
			}
			sparkSession.close();
			System.out.println("SparkSession for Member is closed.");
		}

		protected static void laodMemberFile(SparkSession spark)
				throws SparkException, AnalysisException, FileNotFoundException, InterruptedException {

			Long startTime = System.currentTimeMillis();
			StructType memberSchema = new StructType(
					new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.StringType, false),
							DataTypes.createStructField("Column3", DataTypes.StringType, false),
							DataTypes.createStructField("Column4", DataTypes.StringType, false),
							DataTypes.createStructField("Column5", DataTypes.IntegerType, true),
							DataTypes.createStructField("Column6", DataTypes.StringType, true),
							DataTypes.createStructField("Column7", DataTypes.StringType, false),
							DataTypes.createStructField("Column8", DataTypes.DateType, false),
							DataTypes.createStructField("Column9", DataTypes.StringType, true),
							DataTypes.createStructField("Column10", DataTypes.DateType, false),
							DataTypes.createStructField("Column11", DataTypes.DateType, false) });

			Dataset<Row> memberDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
					.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
					.option("dateFormat", "yyyymmdd").schema(memberSchema).csv(memberGzFile);

			memberDf.show(30);
			memberDf.count();
			memberDf.write().format("csv").save(csvMemberDirectory);

			// read from sftp -- read to spark df and load to cvs table in the desired
			// location
			/*
			 * String fileNameFromECG =
			 * SparkUtility.getFilesNamesFromECG("MBR.0001.Member2*.gz"); Dataset<Row>
			 * sftpMemberDf = spark.read(). format("com.springml.spark.sftp").
			 * option("host", "ecgpi.healthtechnologygroup.com").
			 * option("username","is00c5m"). option("password", "91GGsnma").
			 * option("fileType", "gz"). load("aceudw/dev/"+fileNameFromECG);
			 * 
			 * sftpMemberDf.show();
			 * 
			 * sftpMemberDf.write().format("csv").save(csvDirectory);
			 */
			// df.createTempView("member");

			/*
			 * monotonically_increasing_id MBR.Member10MRows19.gz 99MB uncompressed - 865 MB
			 * MBR.0001.Member.25Rows.txt -- 45056 rows - 11.2 seconds for first time and
			 * 6.7 sec for next runs, Member10000rows.txt option("timestampFormat",
			 * "yyyyMMdd HH:mi:ss") .option("dateFormat", "yyyymmdd") Member1000000Rows.txt
			 * - 86MB -- 15.12 sec -- home network -- 59.8 sec - with indexes - 62 sec, 42
			 * secs, 38 sec Member10MRows.txt -- 864MB -- 136.659747 s -- 2.3 minutes, with
			 * indexes : 5.8 minutes
			 */

			/*
			 * df.createTempView("member"); Dataset<Row> badRecordsDf =
			 * spark.sql("select * from member where GIV_NM is null");
			 * badRecordsDf.show(100);
			 */

			/*
			 * Comparison between compressed and uncompressed with Indexes
			 * MBR.Member10MRows19.gz -- 99MB uncompressed - 865 MB - Member10MRows.txt -
			 * 864 MB -- 9.4 minutes MBR.Member10MRows.txt - 865 MB --
			 */
			try {
				membCsvFile = SparkUtility.getCsvFile(csvMemberDirectory);
			} catch (FileNotFoundException e) {
				throw new FileNotFoundException();
			}
			Dataset<Row> memberCsvDf = sparkSession.read().format("csv").option("header", false)
					.option("delimiter", ",").option("ignoreLeadingWhiteSpace", true)
					.option("ignoreTrailingWhiteSpace", true).option("dateFormat", "yyyymmdd").schema(memberSchema)
					.csv(membCsvFile).withColumn("ID", functions.monotonically_increasing_id())
					.where("GIV_NM is not null");

			membTableView = dbUtility.getView("MEMBER");
			membTableToLoad = membTableView.equalsIgnoreCase(MEMBER_A) ? MEMBER_B : MEMBER_A;
			System.out.println(membTableView + " table is live, truncating and loading on to " + membTableToLoad);
			dbUtility.truncateTable(membTableToLoad);
			membLoadStart = new Date(System.currentTimeMillis());
			dbUtility.saveLoadHistroy(membTableToLoad, membLoadStart, null, "Load Started", null, memberGzFile,
					SparkUtility.getFileSizeinGB(membCsvFile));
			System.out.println("Starting load on " + membTableToLoad);

			try {
				memberCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
						.option("dbtable", membTableToLoad).option("user", dbUser).option("password", dbPass).save();
			} catch (Exception exception) {
				membFinishTime = new Date(System.currentTimeMillis());
				//
				dbUtility.updateLoadHistroy(membTableToLoad, membLoadStart, membFinishTime, "Load Failed", "N",
						memberGzFile);
				throw new SparkException("Exception occured while loading to MemberTable table", exception);
			}

			membFinishTime = new Date(System.currentTimeMillis());

			dbUtility.updateLoadHistroy(membTableToLoad, membLoadStart, membFinishTime, "Load Completed sucessfulluy",
					"Y", memberGzFile);
			Long elapsedtime = System.currentTimeMillis() - startTime;
			System.out.println("Time taken to load : " + elapsedtime);

		}
	}

	public static class LoadMemberAltId implements Runnable {

		static String membAtlIDTableToLoad = null;
		static String membAtlIDTableView = null;
		static Date membAtlIDLoadStart = null;
		static Date membAtlIDFinishTime = null;
		static String membAtlIDCsvFile = null;

		@Override
		public void run() {
			sparkSession = new SparkSession.Builder().master("local[*]").appName("Java JDBC Spark").getOrCreate();
			System.out.println("SparkSession for MemberAlternateID is created.");

			try {
				loadMemberAltIdFile(sparkSession);
			} catch (SparkException | AnalysisException | FileNotFoundException e) {
				// TODO Auto-generated catch block

				// send failure email12222221
				e.printStackTrace();
			} finally {

				try {
					SparkUtility.deleteDirectory(csvMemberAtlDirectory);
				} catch (IOException e) {
					System.out.println("Error occured while deleting Directory : " + csvMemberAtlDirectory);
				}

			}

			sparkSession.close();
			System.out.println("SparkSession for MemberAlternateID is closed.");
		}

		protected static void loadMemberAltIdFile(SparkSession spark)
				throws SparkException, AnalysisException, FileNotFoundException {

			Long startTime = System.currentTimeMillis();
			StructType memberAltIDschema = new StructType(
					new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.StringType, false),
							DataTypes.createStructField("Column3", DataTypes.StringType, false) });
			Dataset<Row> memberAltDf = sparkSession.read().format("csv").option("header", false)
					.option("delimiter", ",").option("ignoreLeadingWhiteSpace", true)
					.option("ignoreTrailingWhiteSpace", true).option("dateFormat", "yyyymmdd").schema(memberAltIDschema)
					.csv(memberAltIdGzFile);

			memberAltDf.show(30);
			memberAltDf.count();
			memberAltDf.write().format("csv").save(csvMemberAtlDirectory);

			try {
				membAtlIDCsvFile = SparkUtility.getCsvFile(csvMemberAtlDirectory);
			} catch (FileNotFoundException e) {
				throw new FileNotFoundException();
			}

			Dataset<Row> memberAltCsvDf = sparkSession.read().format("csv").option("header", false)
					.option("delimiter", ",").option("ignoreLeadingWhiteSpace", true)
					.option("ignoreTrailingWhiteSpace", true).option("dateFormat", "yyyymmdd").schema(memberAltIDschema)
					.csv(membAtlIDCsvFile).withColumn("ID", functions.monotonically_increasing_id());

			membAtlIDTableView = dbUtility.getView("MEMBERALTERNATE");
			membAtlIDTableToLoad = membAtlIDTableView.equalsIgnoreCase(MEMBERALTERNATE_A) ? MEMBERALTERNATE_B
					: MEMBERALTERNATE_A;
			System.out.println(
					membAtlIDTableView + " table is live, truncating and loading on to " + membAtlIDTableToLoad);
			dbUtility.truncateTable(membAtlIDTableToLoad);
			membAtlIDLoadStart = new Date(System.currentTimeMillis());
			dbUtility.saveLoadHistroy(membAtlIDTableToLoad, membAtlIDLoadStart, null, "Load Started", null,
					memberAltIdGzFile, SparkUtility.getFileSizeinGB(membAtlIDCsvFile));
			System.out.println("Starting load on " + membAtlIDTableToLoad);
			try {
				memberAltCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
						.option("dbtable", membAtlIDTableToLoad).option("user", dbUser).option("password", dbPass)
						.save();
			} catch (Exception exception) {
				membAtlIDFinishTime = new Date(System.currentTimeMillis());

				dbUtility.updateLoadHistroy(membAtlIDTableToLoad, membAtlIDLoadStart, membAtlIDFinishTime,
						"Load Failed", "N", memberAltIdGzFile);
				throw new SparkException("Exception occured while loading to MemberAlternate table", exception);
			}
			membAtlIDFinishTime = new Date(System.currentTimeMillis());
			dbUtility.updateLoadHistroy(membAtlIDTableToLoad, membAtlIDLoadStart, membAtlIDFinishTime,
					"Load Completed sucessfulluy", "Y", memberAltIdGzFile);
			Long elapsedtime = System.currentTimeMillis() - startTime;
			System.out.println("Time taken to load : " + elapsedtime);

		}
	}

	public static class LaadMemberCoverage implements Runnable {

		static String membCovTableToLoad = null;
		static String membCovTableView = null;
		static Date membCovLoadStart = null;
		static Date membCovFinishTime = null;
		static String membCovCsvFile = null;

		@Override
		public void run() {
			sparkSession = new SparkSession.Builder().master("local[*]").appName("Java JDBC Spark").getOrCreate();
			System.out.println("SparkSession for MemberCoverage is created.");
			try {
				laodMemberCoverageFile(sparkSession);
			} catch (SparkException | AnalysisException | IOException | InterruptedException e) {

				System.out.println("Exception occured in memberCoverage :" + e);
				// send email notification
				e.printStackTrace();
			} finally {

				try {
					SparkUtility.deleteDirectory(csvMemberCovDirectory);
				} catch (IOException e) {
					System.out.println("Error occured while deleting Directory : " + csvMemberCovDirectory);
				}

			}
			sparkSession.close();
			System.out.println("SparkSession for MemberCoverage is closed.");
		}

		protected static void laodMemberCoverageFile(SparkSession spark)
				throws SparkException, AnalysisException, FileNotFoundException, InterruptedException {

			Long startTime = System.currentTimeMillis();
			StructType memberCovSchema = new StructType(
					new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.DateType, true),
							DataTypes.createStructField("Column3", DataTypes.DateType, true),
							DataTypes.createStructField("Column4", DataTypes.StringType, true),
							DataTypes.createStructField("Column5", DataTypes.StringType, true),
							DataTypes.createStructField("Column6", DataTypes.DateType, true),
							DataTypes.createStructField("Column7", DataTypes.DateType, true),
							DataTypes.createStructField("Column8", DataTypes.StringType, true),
							DataTypes.createStructField("Column9", DataTypes.StringType, true),
							DataTypes.createStructField("Column10", DataTypes.StringType, true),
							DataTypes.createStructField("Column11", DataTypes.DateType, true),
							DataTypes.createStructField("Column12", DataTypes.StringType, true),
							DataTypes.createStructField("Column13", DataTypes.StringType, true),
							DataTypes.createStructField("Column14", DataTypes.StringType, true),
							DataTypes.createStructField("Column15", DataTypes.StringType, true) });

			Dataset<Row> membCovDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
					.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
					.option("dateFormat", "yyyymmdd").schema(memberCovSchema).csv(memberCoverageGzFile);

			membCovDf.show(30);
			membCovDf.count();
			membCovDf.write().format("csv").save(csvMemberCovDirectory);
			try {
				membCovCsvFile = SparkUtility.getCsvFile(csvMemberCovDirectory);
			} catch (FileNotFoundException e) {
				throw new FileNotFoundException();
			}
			Dataset<Row> membCovCsvDf = sparkSession.read().format("csv").option("header", false)
					.option("delimiter", ",").option("ignoreLeadingWhiteSpace", true)
					.option("ignoreTrailingWhiteSpace", true).option("dateFormat", "yyyymmdd").schema(memberCovSchema)
					.csv(membCovCsvFile).withColumn("ID", functions.monotonically_increasing_id());

			membCovTableView = dbUtility.getView("MEMBERCOVERAGE");
			membCovTableToLoad = membCovTableView.equalsIgnoreCase(MEMBERCOVERAGE_A) ? MEMBERCOVERAGE_B
					: MEMBERCOVERAGE_A;
			System.out.println(membCovTableView + " table is live, truncating and loading on to " + membCovTableToLoad);
			dbUtility.truncateTable(membCovTableToLoad);
			membCovLoadStart = new Date(System.currentTimeMillis());
			dbUtility.saveLoadHistroy(membCovTableToLoad, membCovLoadStart, null, "Load Started", null,
					memberCoverageGzFile, SparkUtility.getFileSizeinGB(membCovCsvFile));
			System.out.println("Starting load on " + membCovTableToLoad);

			try {
				membCovCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
						.option("dbtable", membCovTableToLoad).option("user", dbUser).option("password", dbPass).save();
			} catch (Exception exception) {
				membCovFinishTime = new Date(System.currentTimeMillis());

				dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime, "Load Failed", "N",
						memberCoverageGzFile);
				throw new SparkException("Exception occured while loading to MemberCoverage table", exception);
			}

			membCovFinishTime = new Date(System.currentTimeMillis());

			dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime,
					"Load Completed sucessfulluy", "Y", memberCoverageGzFile);
			Long elapsedtime = System.currentTimeMillis() - startTime;
			System.out.println("Time taken to load : " + elapsedtime);

		}
	}

	public void loadMemberSeq(SparkSession spark)
			throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {

		String membTableToLoad1 = null;
		String membTableView1 = null;
		Date membLoadStart1 = null;
		Date membFinishTime1 = null;
		String membCsvFile1 = null;
		Long startTime = System.currentTimeMillis();
		StructType memberSchema = new StructType(
		new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.StringType, false),
							DataTypes.createStructField("Column3", DataTypes.StringType, false),
							DataTypes.createStructField("Column4", DataTypes.StringType, false),
							DataTypes.createStructField("Column5", DataTypes.IntegerType, true),
							DataTypes.createStructField("Column6", DataTypes.StringType, true),
							DataTypes.createStructField("Column7", DataTypes.StringType, false),
							DataTypes.createStructField("Column8", DataTypes.DateType, false),
							DataTypes.createStructField("Column9", DataTypes.StringType, true),
							DataTypes.createStructField("Column10", DataTypes.DateType, false),
							DataTypes.createStructField("Column11", DataTypes.DateType, false) });


	/*	Dataset<Row> memberDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
				.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
				.option("dateFormat", "yyyymmdd").schema(memberSchema).csv(memberGzFile);

		memberDf.show(30);
		memberDf.count();
		memberDf.write().format("csv").save(csvMemberDirectory);*/

		// read from sftp -- read to spark df and load to cvs table in the desired
		// location

		
		String fileNameFromECG =
		 SparkUtility.getFileNamesFromECG("MBR.0001.Member.[0-9].*gz"); 
		Dataset<Row>
		 sftpMemberDf = spark.read().format("com.springml.spark.sftp") .option("host",
		 "ecgpi.healthtechnologygroup.com").option("username", "is00c5m")
		  .option("password", "91GGsnma").option("fileType",
		 "csv").schema(memberSchema) .load("aceudw/dev/" + fileNameFromECG);
		 
		 sftpMemberDf.show();
		 
		 sftpMemberDf.write().format("csv").save(csvMemberDirectory);
		 

		try {
			membCsvFile1 = SparkUtility.getCsvFile(csvMemberDirectory);
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException();
		}
		Dataset<Row> memberCsvDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
				.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
				.option("dateFormat", "yyyymmdd").schema(memberSchema).csv(membCsvFile1)
				.withColumn("ID", functions.monotonically_increasing_id()).where("GIV_NM is not null");

		membTableView1 = dbUtility.getView("MEMBER");
		membTableToLoad1 = membTableView1.equalsIgnoreCase(MEMBER_A) ? MEMBER_B : MEMBER_A;
		System.out.println(membTableView1 + " table is live, truncating and loading on to " + membTableToLoad1);
		dbUtility.truncateTable(membTableToLoad1);
		membLoadStart1 = new Date(System.currentTimeMillis());
		dbUtility.saveLoadHistroy(membTableToLoad1, membLoadStart1, null, "Load Started", null, fileNameFromECG,
				SparkUtility.getFileSizeinGB(membCsvFile1));
		System.out.println("Starting load on " + membTableToLoad1);

		try {
			memberCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
					.option("dbtable", membTableToLoad1).option("user", dbUser).option("password", dbPass).save();
		} catch (Exception exception) {
			membFinishTime1 = new Date(System.currentTimeMillis());

			dbUtility.updateLoadHistroy(membTableToLoad1, membLoadStart1, membFinishTime1, "Load Failed", "N",
					fileNameFromECG);
			throw new SparkException("Exception occured while loading to MemberTable table", exception);
		}

		membFinishTime1 = new Date(System.currentTimeMillis());

		dbUtility.updateLoadHistroy(membTableToLoad1, membLoadStart1, membFinishTime1, "Load Completed sucessfulluy",
				"Y", fileNameFromECG);
		Long elapsedtime = System.currentTimeMillis() - startTime;
		System.out.println("Time taken to load : " + elapsedtime);

	}

	public void loadMemberAltSeq(SparkSession spark)
			throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {

		String membAtlIDTableToLoad1 = null;
		String membAtlIDTableView1 = null;
		Date membAtlIDLoadStart1 = null;
		Date membAtlIDFinishTime1 = null;
		String membAtlIDCsvFile1 = null;

		Long startTime = System.currentTimeMillis();
		StructType memberAltIDschema = new StructType(
				new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.StringType, false),
							DataTypes.createStructField("Column3", DataTypes.StringType, false) });
		/*
		 * Dataset<Row> memberAltDf = sparkSession.read().format("csv").option("header",
		 * false).option("delimiter", ",") .option("ignoreLeadingWhiteSpace",
		 * true).option("ignoreTrailingWhiteSpace", true) .option("dateFormat",
		 * "yyyymmdd").schema(memberAltIDschema).csv(memberAltIdGzFile);
		 * 
		 * 
		 * memberAltDf.show(30); memberAltDf.count();
		 * memberAltDf.write().format("csv").save(csvMemberAtlDirectory);
		 */
		String fileNameFromECG = SparkUtility.getFileNamesFromECG("MBR.0001.MemberAlternateID.[0-9].*gz");
		Dataset<Row> sftpMemberAltDf = spark.read().format("com.springml.spark.sftp")
				.option("host", "ecgpi.healthtechnologygroup.com").option("username", "is00c5m")
				.option("password", "91GGsnma").option("fileType", "csv").schema(memberAltIDschema)
				.load("aceudw/dev/" + fileNameFromECG);

		sftpMemberAltDf.show();

		sftpMemberAltDf.write().format("csv").save(csvMemberAtlDirectory);

		try {
			membAtlIDCsvFile1 = SparkUtility.getCsvFile(csvMemberAtlDirectory);
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException();
		}

		Dataset<Row> memberAltCsvDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
				.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
				.option("dateFormat", "yyyymmdd").schema(memberAltIDschema).csv(membAtlIDCsvFile1)
				.withColumn("ID", functions.monotonically_increasing_id());

		membAtlIDTableView1 = dbUtility.getView("MEMBERALTERNATE");
		membAtlIDTableToLoad1 = membAtlIDTableView1.equalsIgnoreCase(MEMBERALTERNATE_A) ? MEMBERALTERNATE_B
				: MEMBERALTERNATE_A;
		System.out
				.println(membAtlIDTableView1 + " table is live, truncating and loading on to " + membAtlIDTableToLoad1);
		dbUtility.truncateTable(membAtlIDTableToLoad1);
		membAtlIDLoadStart1 = new Date(System.currentTimeMillis());
		dbUtility.saveLoadHistroy(membAtlIDTableToLoad1, membAtlIDLoadStart1, null, "Load Started", null,
				fileNameFromECG, SparkUtility.getFileSizeinGB(membAtlIDCsvFile1));
		System.out.println("Starting load on " + membAtlIDTableToLoad1);
		try {
			memberAltCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
					.option("dbtable", membAtlIDTableToLoad1).option("user", dbUser).option("password", dbPass).save();
		} catch (Exception exception) {
			membAtlIDFinishTime1 = new Date(System.currentTimeMillis());

			dbUtility.updateLoadHistroy(membAtlIDTableToLoad1, membAtlIDLoadStart1, membAtlIDFinishTime1, "Load Failed",
					"N", fileNameFromECG);
			throw new SparkException("Exception occured while loading to MemberAlternate table", exception);
		}
		membAtlIDFinishTime1 = new Date(System.currentTimeMillis());
		dbUtility.updateLoadHistroy(membAtlIDTableToLoad1, membAtlIDLoadStart1, membAtlIDFinishTime1,
				"Load Completed sucessfulluy", "Y", fileNameFromECG);
		Long elapsedtime = System.currentTimeMillis() - startTime;
		System.out.println("Time taken to load : " + elapsedtime);

	}

	public void loadMemberCovSeq(SparkSession spark)
			throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {

		String membCovTableToLoad = null;
		String membCovTableView = null;
		Date membCovLoadStart = null;
		Date membCovFinishTime = null;
		String membCovCsvFile = null;

		Long startTime = System.currentTimeMillis();
		StructType memberCovSchema = new StructType(
						new StructField[] { DataTypes.createStructField("Column1", DataTypes.StringType, false),
							DataTypes.createStructField("Column2", DataTypes.DateType, true),
							DataTypes.createStructField("Column3", DataTypes.DateType, true),
							DataTypes.createStructField("Column4", DataTypes.StringType, true),
							DataTypes.createStructField("Column5", DataTypes.StringType, true),
							DataTypes.createStructField("Column6", DataTypes.DateType, true),
							DataTypes.createStructField("Column7", DataTypes.DateType, true),
							DataTypes.createStructField("Column8", DataTypes.StringType, true),
							DataTypes.createStructField("Column9", DataTypes.StringType, true),
							DataTypes.createStructField("Column10", DataTypes.StringType, true),
							DataTypes.createStructField("Column11", DataTypes.DateType, true),
							DataTypes.createStructField("Column12", DataTypes.StringType, true),
							DataTypes.createStructField("Column13", DataTypes.StringType, true),
							DataTypes.createStructField("Column14", DataTypes.StringType, true),
							DataTypes.createStructField("Column15", DataTypes.StringType, true) });


		/*
		 * Dataset<Row> membCovDf = sparkSession.read().format("csv").option("header",
		 * false).option("delimiter", ",") .option("ignoreLeadingWhiteSpace",
		 * true).option("ignoreTrailingWhiteSpace", true) .option("dateFormat",
		 * "yyyymmdd").schema(memberCovSchema) .csv(memberCoverageGzFile);
		 * 
		 * membCovDf.show(30); membCovDf.count();
		 * membCovDf.write().format("csv").save(csvMemberCovDirectory);
		 */

		String fileNameFromECG = SparkUtility.getFileNamesFromECG("MBR.0001.MemberCoverage.[0-9].*gz");
		Dataset<Row> sftpMemberCovDf = spark.read().format("com.springml.spark.sftp")
				.option("host", "ecgpi.healthtechnologygroup.com").option("username", "is00c5m")
				.option("password", "91GGsnma").option("fileType", "csv").schema(memberCovSchema)
				.load("aceudw/dev/" + fileNameFromECG);

		sftpMemberCovDf.show();

		sftpMemberCovDf.write().format("csv").save(csvMemberAtlDirectory);
		try {
			membCovCsvFile = SparkUtility.getCsvFile(csvMemberCovDirectory);
		} catch (FileNotFoundException e) {
			throw new FileNotFoundException();
		}
		Dataset<Row> membCovCsvDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
				.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
				.option("dateFormat", "yyyymmdd").schema(memberCovSchema).csv(membCovCsvFile)
				.withColumn("ID", functions.monotonically_increasing_id());

		membCovTableView = dbUtility.getView("MEMBERCOVERAGE");
		membCovTableToLoad = membCovTableView.equalsIgnoreCase(MEMBERCOVERAGE_A) ? MEMBERCOVERAGE_B : MEMBERCOVERAGE_A;
		System.out.println(membCovTableView + " table is live, truncating and loading on to " + membCovTableToLoad);
		dbUtility.truncateTable(membCovTableToLoad);
		membCovLoadStart = new Date(System.currentTimeMillis());
		dbUtility.saveLoadHistroy(membCovTableToLoad, membCovLoadStart, null, "Load Started", null,
				memberCoverageGzFile, SparkUtility.getFileSizeinGB(membCovCsvFile));
		System.out.println("Starting load on " + membCovTableToLoad);

		try {
			membCovCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
					.option("dbtable", membCovTableToLoad).option("user", dbUser).option("password", dbPass).save();
		} catch (Exception exception) {
			membCovFinishTime = new Date(System.currentTimeMillis());

			dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime, "Load Failed", "N",
					memberCoverageGzFile);
			throw new SparkException("Exception occured while loading to MemberCoverage table", exception);
		}

		membCovFinishTime = new Date(System.currentTimeMillis());

		dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime,
				"Load Completed sucessfulluy", "Y", memberCoverageGzFile);
		Long elapsedtime = System.currentTimeMillis() - startTime;
		System.out.println("Time taken to load : " + elapsedtime);

	}

	public static void main(String args[])
			throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {
		sparkSession = new SparkSession.Builder().master("local[*]").appName("Java JDBC Spark").getOrCreate();
		SparkSqlSession session = new SparkSqlSession();
		try {
		session.loadMemberSeq(sparkSession);
		
		session.loadMemberAltSeq(sparkSession);
	// session.loadMemberCovSeq(sparkSession);
		 
		}catch(Exception e) {
	
		}finally {
			
		
		
		sparkSession.close();
		}
	}
}
