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
	private static String sftpHost;
	private static String sftpUser;
	private static String sftpPass;

	static {
		dbUser=properties.getDbUser();
		dbPass=properties.getDbPass();
		dbUrl=properties.getDbUrl();
		sftpHost=properties.getSftpHost();
		sftpUser=properties.getSftpUser();
		sftpPass=properties.getSftpPass();
	}
	public static void main(String args[]) throws InterruptedException {
			Callable<Object> memberCall = Executors.callable(new LaadMember());
			Callable<Object> memberAltIdCall = Executors.callable(new LoadMemberAltId());
			Callable<Object> memberCovCall = Executors.callable(new LaadMemberCoverage());
			Set<Callable<Object>> calls = new HashSet<>();
			calls.add(memberCall);
			calls.add(memberAltIdCall);
			calls.add(memberCovCall);

			ExecutorService service = Executors.newCachedThreadPool();
			// ExecutorService service = Executors.newFixedThreadPool(3);

			service.invokeAll(calls);
			service.shutdown();
			System.exit(0);

		}

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
				} catch (SparkException | AnalysisException | InterruptedException | IOException | JSchException | SftpException e) {

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
			}

			protected static void laodMemberFile(SparkSession spark)
					throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {

				Long startTime = System.currentTimeMillis();
				StructType memberSchema = new StructType(
						new StructField[] { DataTypes.createStructField("MBR_PTY_ID", DataTypes.StringType, false),
								DataTypes.createStructField("GIV_NM", DataTypes.StringType, false),
								DataTypes.createStructField("FAM_NM", DataTypes.StringType, false),
								DataTypes.createStructField("SRC_SBSCR_ID", DataTypes.StringType, false),
								DataTypes.createStructField("DEPN_NBR", DataTypes.IntegerType, true),
								DataTypes.createStructField("DEPN_SEQ_NUM", DataTypes.StringType, true),
								DataTypes.createStructField("REL_CD", DataTypes.StringType, false),
								DataTypes.createStructField("BTH_DT", DataTypes.DateType, false),
								DataTypes.createStructField("GDR_TYP_CD", DataTypes.StringType, true),
								DataTypes.createStructField("MBR_ROW_EFF_DT", DataTypes.DateType, false),
								DataTypes.createStructField("MBR_ROW_EXPIR_DT", DataTypes.DateType, false) });

				/*Dataset<Row> memberDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
						.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
						.option("dateFormat", "yyyymmdd").schema(memberSchema).csv(memberGzFile);

				memberDf.show(30);
				memberDf.count();
				memberDf.write().format("csv").save(csvMemberDirectory);*/
				String fileNameFromECG =
						 SparkUtility.getFileNamesFromECG("MBR.0001.Member.[0-9].*gz");
						Dataset<Row>
						 sftpMemberDf = spark.read().format("com.springml.spark.sftp") .option("host",
						 sftpHost).option("username", sftpUser)
						  .option("password", sftpPass).option("fileType",
						 "csv").schema(memberSchema) .load("aceudw/dev/" + fileNameFromECG);

						 sftpMemberDf.show();

						 sftpMemberDf.write().format("csv").save(csvMemberDirectory);


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
				dbUtility.saveLoadHistroy(membTableToLoad, membLoadStart, null, "Load Started", null, fileNameFromECG,
						SparkUtility.getFileSizeinGB(membCsvFile));
				System.out.println("Starting load on " + membTableToLoad);

				try {
					memberCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
							.option("dbtable", membTableToLoad).option("user", dbUser).option("password", dbPass).save();
				} catch (Exception exception) {
					membFinishTime = new Date(System.currentTimeMillis());
					//
					dbUtility.updateLoadHistroy(membTableToLoad, membLoadStart, membFinishTime, "Load Failed", "N",
							fileNameFromECG);
					throw new SparkException("Exception occured while loading to MemberTable table", exception);
				}

				membFinishTime = new Date(System.currentTimeMillis());

				dbUtility.updateLoadHistroy(membTableToLoad, membLoadStart, membFinishTime, "Load Completed sucessfulluy",
						"Y", fileNameFromECG);
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
				} catch (SparkException | AnalysisException | IOException | JSchException | SftpException e) {
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

			}

			protected static void loadMemberAltIdFile(SparkSession spark)
					throws SparkException, AnalysisException, IOException, JSchException, SftpException {

				Long startTime = System.currentTimeMillis();
				StructType memberAltIDschema = new StructType(
						new StructField[] { DataTypes.createStructField("MBR_PTY_ID", DataTypes.StringType, false),
								DataTypes.createStructField("ALT_ID_TYP_CD", DataTypes.StringType, false),
								DataTypes.createStructField("ALT_ID_VAL", DataTypes.StringType, false) });
			/*	Dataset<Row> memberAltDf = sparkSession.read().format("csv").option("header", false)
						.option("delimiter", ",").option("ignoreLeadingWhiteSpace", true)
						.option("ignoreTrailingWhiteSpace", true).option("dateFormat", "yyyymmdd").schema(memberAltIDschema)
						.csv(memberAltIdGzFile);

				memberAltDf.show(30);
				memberAltDf.count();
				memberAltDf.write().format("csv").save(csvMemberAtlDirectory);*/

				String fileNameFromECG = SparkUtility.getFileNamesFromECG("MBR.0001.MemberAlternateID.[0-9].*gz");
				Dataset<Row> sftpMemberAltDf = spark.read().format("com.springml.spark.sftp")
						.option("host", sftpHost).option("username", sftpUser)
						.option("password", sftpPass).option("fileType", "csv").schema(memberAltIDschema)
						.load("aceudw/dev/" + fileNameFromECG);

				sftpMemberAltDf.show();

				sftpMemberAltDf.write().format("csv").save(csvMemberAtlDirectory);
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
						fileNameFromECG, SparkUtility.getFileSizeinGB(membAtlIDCsvFile));
				System.out.println("Starting load on " + membAtlIDTableToLoad);
				try {
					memberAltCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
							.option("dbtable", membAtlIDTableToLoad).option("user", dbUser).option("password", dbPass)
							.save();
				} catch (Exception exception) {
					membAtlIDFinishTime = new Date(System.currentTimeMillis());

					dbUtility.updateLoadHistroy(membAtlIDTableToLoad, membAtlIDLoadStart, membAtlIDFinishTime,
							"Load Failed", "N", fileNameFromECG);
					throw new SparkException("Exception occured while loading to MemberAlternate table", exception);
				}
				membAtlIDFinishTime = new Date(System.currentTimeMillis());
				dbUtility.updateLoadHistroy(membAtlIDTableToLoad, membAtlIDLoadStart, membAtlIDFinishTime,
						"Load Completed sucessfulluy", "Y", fileNameFromECG);
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
				} catch (SparkException | AnalysisException | IOException | InterruptedException | JSchException | SftpException e) {

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
			}

			protected static void laodMemberCoverageFile(SparkSession spark)
					throws SparkException, AnalysisException, InterruptedException, IOException, JSchException, SftpException {

				Long startTime = System.currentTimeMillis();
				StructType memberCovSchema = new StructType(
						new StructField[] { DataTypes.createStructField("MBR_PTY_ID", DataTypes.StringType, false),
								DataTypes.createStructField("COV_EFF_DT", DataTypes.DateType, true),
								DataTypes.createStructField("COV_EXPIR_DT", DataTypes.DateType, true),
								DataTypes.createStructField("GOVT_PGM_TYP_CD", DataTypes.StringType, true),
								DataTypes.createStructField("SRC_FUND_ARNG_CD", DataTypes.StringType, true),
								DataTypes.createStructField("COV_ROW_EFF_DT", DataTypes.DateType, true),
								DataTypes.createStructField("COV_ROW_EXPIR_DT", DataTypes.DateType, true),
								DataTypes.createStructField("SRC_MBR_ID", DataTypes.StringType, true),
								DataTypes.createStructField("SRC_LGCY_POL_NUM", DataTypes.StringType, true),
								DataTypes.createStructField("SRC_SYS_PRDCT_TYP_CD", DataTypes.StringType, true),
								DataTypes.createStructField("SRC_COV_EFF_DT", DataTypes.DateType, true),
								DataTypes.createStructField("SRC_COV_TYP_CD", DataTypes.StringType, true),
								DataTypes.createStructField("CDB_SRC_SYS_CD", DataTypes.StringType, true),
								DataTypes.createStructField("SRC_BEN_PLN_ID", DataTypes.StringType, true),
								DataTypes.createStructField("ORIG_SRC_SYS_PRDCT_CD", DataTypes.StringType, true) });

				/*Dataset<Row> membCovDf = sparkSession.read().format("csv").option("header", false).option("delimiter", ",")
						.option("ignoreLeadingWhiteSpace", true).option("ignoreTrailingWhiteSpace", true)
						.option("dateFormat", "yyyymmdd").schema(memberCovSchema).csv(memberCoverageGzFile);

				membCovDf.show(30);
				membCovDf.count();
				membCovDf.write().format("csv").save(csvMemberCovDirectory);*/

				String fileNameFromECG = SparkUtility.getFileNamesFromECG("MBR.0001.MemberCoverage.[0-9].*gz");
				Dataset<Row> sftpMemberCovDf = spark.read().format("com.springml.spark.sftp")
						.option("host", sftpHost).option("username", sftpUser)
						.option("password", sftpPass).option("fileType", "csv").schema(memberCovSchema)
						.load("aceudw/dev/" + fileNameFromECG);

				sftpMemberCovDf.show();

				sftpMemberCovDf.write().format("csv").save(csvMemberAtlDirectory);
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
						fileNameFromECG, SparkUtility.getFileSizeinGB(membCovCsvFile));
				System.out.println("Starting load on " + membCovTableToLoad);

				try {
					membCovCsvDf.write().mode(SaveMode.Append).format("jdbc").option("url", dbUrl)
							.option("dbtable", membCovTableToLoad).option("user", dbUser).option("password", dbPass).save();
				} catch (Exception exception) {
					membCovFinishTime = new Date(System.currentTimeMillis());

					dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime, "Load Failed", "N",
							fileNameFromECG);
					throw new SparkException("Exception occured while loading to MemberCoverage table", exception);
				}

				membCovFinishTime = new Date(System.currentTimeMillis());

				dbUtility.updateLoadHistroy(membCovTableToLoad, membCovLoadStart, membCovFinishTime,
						"Load Completed sucessfulluy", "Y", fileNameFromECG);
				Long elapsedtime = System.currentTimeMillis() - startTime;
				System.out.println("Time taken to load : " + elapsedtime);

						}
					}
				}
