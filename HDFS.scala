
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import java.io.File
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{SaveMode, SparkSession, SQLContext

import org.apache.spark.sql.execution.datasources.hbase._


object HDFS {

//Copy Local Files to HDFS in HDP
  def copyFiles(localPath: String, remotePath: String, hdfs: FileSystem): Unit = {
    val localFiles = getListOfFiles(localPath)
    localFiles.foreach( localPath =>  hdfs.copyFromLocalFile(new Path(localPath.toString), new Path(remotePath + localPath.getName)))

  }

  //Get all files in HDFS
  def getListOfFiles(hdfs: FileSystem, path: String): List[FileStatus]  = {

    val files = hdfs.listStatus(new Path(path))
    files.toList
  }

  //Get all files in local directory
  def getListOfFiles(dir: String):List[File] = {
    val file = new File(dir)
    if (file.exists && file.isDirectory) {
      file.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  //Load tables from cvs file in HDFS to Hive
  def loadTable(path: Path, sparkSession: SparkSession, hdfs: FileSystem): Unit = {

    val dataFrame= sparkSession.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv(path.toString)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable(path.getName.substring(0,path.getName.indexOf(".")))
 }

	
  def createCatalogString(schema: List[StructField], tableName: String, rowKey: String): String = {
    var cols = ""
    var i = 0
      schema.foreach(row => {var rType=""; if (row.dataType.toString == "TimestampType") rType ="STRING"; else rType =row.dataType.sql; if (i == 0) cols = cols + "\n       |\""+rowKey +  "\":{\"cf\":\"rowkey\""+
        ", \"col\":\""+row.name+"\", \"type\":\""+rType+"\"}," ;
        else
        cols = cols + "\n       |\"" + row.name + "\":{\"cf\":\"cf"+i+
        "\", \"col\":\""+row.name+"\", \"type\":\"" +rType+"\"}," ; i=i+1;})
    var header = "\n       |\"table\":{\"namespace\":\"default\", \"name\":\""+ tableName +"\"},\n" +
      "       |\"rowkey\":\""+rowKey+"\",\n       |\"columns\":{\n  "
    header = "{" + header+cols.substring(0,cols.length-1)+ "\n       |}\n     |}"
    header.toString
  }

  def main(args: Array[String]): Unit = {

    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)



    var localPath= "/home/carlos/Documents/data-spark/"
    var remotePath = "/data-spark/"
    val DFS = "hdfs://sandbox-hdp.hortonworks.com:8020"

    //Get HDP Files system (HDFS)
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.defaultFS", DFS)
    val hdfs = FileSystem.get(hadoopConf)

    //Copy Hive Files from local path to HDFS
   copyFiles(localPath,remotePath, hdfs )

  //Create sparkSession
    val warehouseLocation = hdfs.getUri + "/apps/hive/warehouse"
    val sparkSession = SparkSession.builder
        .master("local")
      .appName("TableToHive")
      .config("spark.sql.warehouse.dir",warehouseLocation)
      .config("hive.metastore.warehouse.dir", "thrift://sandbox-hdp.hortonworks.com:9083")
      .enableHiveSupport()
      .getOrCreate()

    //Get Path of files in HDFS to create tables in Hive
    var hdpFiles = getListOfFiles(hdfs, remotePath)
      hdpFiles.foreach(file=> loadTable(file.getPath, sparkSession, hdfs))

    //Output data frame with data from hive tables
      import sparkSession.sql
      val dfHive = sql("SELECT d.DriverId, d.Name, t.Hours_Logged, t.Miles_Logged from drivers d " +
        "INNER JOIN (SELECT DriverId, sum(`hours-logged`) as Hours_Logged , sum(`miles-logged`) as Miles_Logged " +
        "FROM timesheet GROUP BY DriverId ) t ON (d.DriverId = t.DriverId)")
      dfHive.show()
  

/*
---------------HBASE---------------
*/

	//Copy Files from local path to HDFS
    localPath= "/home/carlos/Documents/data-hbase/"
    remotePath = "/data-hbase/"
    copyFiles(localPath,remotePath, hdfs )

	//Load dangerous-driver.csv in a dataframe without inferSchema
    var path = new Path(DFS + remotePath +"/dangerous-driver.csv")
    var dataFrame= sparkSession.read
      .option("header","true")
      //.option("inferSchema", "true") //Had problem with Timestamp datatype
      .csv(path.toString)

	  //Define a Catalog, a schema to load the dataframe data into a HBase Table
	val rowKey = "eventId"  
    val stringCatalog = createCatalogString(dataFrame.schema.toList, path.getName.substring(0,path.getName.indexOf(".")),rowKey) 
    def catalog = stringCatalog.stripMargin

	//Create table in HBase
    dataFrame
      .write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

	//Load data from extra-driver.csv to an dataframe 
    path = new Path(DFS + remotePath +"/extra-driver.csv")
    dataFrame= sparkSession.read
      .option("header","true")
      .csv(path.toString)


	/*
		The new row to be added to the dangerous-driver table has the same eventId of and existing one
		so if I don't change this eventId it will overwrite the existing one.
		
		This solution only works for this example data, because if it contains more rows, they can overwrite existing
		rows if they have same eventId. A solution could be creating a new column with a different and autoincrement ID
	*/
    dataFrame.na.replace("eventId",Map("1"->"4"))
      .write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()



// Create a dataframe from the HBase table including SparkSQL context to query the table with sql
     var df  = sparkSession.sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog->catalog))
        .format("org.apache.spark.sql.execution.datasources.hbase")
        .load()

    
//Replace routeName  Los Angeles to Santa Clara for Santa Clara to San Diego in record 4
    df.filter("eventId == \"4\"").na.replace("routeName", Map("Santa Clara to San Diego"->"Los Angeles to Santa Clara"))
      .write
      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

   
//Register a temporary table to query the HBase table using Spark SQL:
    df.registerTempTable("dangerousDriver")
    var query = sparkSession.sqlContext.sql("select driverName, eventType, eventTime from dangerousDriver where routeName like \"Los Angeles to%\"")
      query.show()

    sparkSession.stop()
  }

}
