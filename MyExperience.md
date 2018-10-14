# My experience and further notes

Since I don’t have a laptop with good specifications, I created a free account in Azure to create a Linux Server virtual machine; I did not create an HDP Sandbox directly in Azure because I wanted to install all as it would be in my own machine.


## Creating Linux Server VM

I chose Ubuntu Server 18.04 as my VM because is an OS that I’ve had some experience with Linux in the past; I created it with 4 virtual CPUs and 16GB of RAM. After deploying the server, via the command line available in the VM on Azure portal, I installed the desktop environment and the RDP server, so I could use it as if I was using “local machine”.

I run the following command to update packages DB
    `sudo apt-get update`
    
To install desktop environment
    `sudo apt-get install ubuntu-desktop`
    
Then I installed the xrdp server to connect with the server with my local machine
    `sudo apt-get install xrdp`
    
After that for starting the xrdp server I typed
    `sudo /etc/init.d/xrdp start`
    
Then added a user and gave it superuser access
    `sudo adduser $USER`
    `sudo adduser $USER sudo`

With this and the 3389 port set up in the server for a remote desktop connection, I can now connect to the server as a desktop machine to make all other configurations.

## Docker Installation

To Install Docker I followed the instruction here without any issues.
https://docs.docker.com/install/linux/docker-ce/ubuntu/#set-up-the-repository  


## HDP Sandbox

I decided to install de HDP on Docker so I followed the instructions here:

https://es.hortonworks.com/tutorial/sandbox-deployment-and-install-guide/section/3/

Found some problems that I describe below and how I solve them:

1. I got an error because of lacking permissions to access the unix socket to communicate with the engine.

    *docker: Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Post http://%2Fvar%2Frun%2Fdocker.sock/v1.26/containers/create: dial unix /var/run/docker.sock: connect: permission denied.*

  So, I added the user to the docker group:
    `sudo usermod -a -G docker $USER`

2.	After that, I got an error because the VM root folder was only 30GB and it got full when files where extracting

    *failed to register layer: Error processing tar file(exit status 1): no space left on device*

  I went to azure and then resized the disk from 30GB to 100GB; what I read was that all Linux VM are usually 30GB by default.

3.	Next error was because the shell did not recognize '==' for string equality and hence the 'hostname' variable was not set, resulting in a null string being passed to the 'network-alias' option in docker run command.

  *docker: Error response from daemon: network-scoped alias is supported only for containers in user defined networks.*

  I changed the sh file replacing '==' with '=' in the following if condition code.
    `# start the docker container and proxy
    if [ "$flavor" == "hdf" ]; then
    hostname="sandbox-hdf.hortonworks.com"
    elif [ "$flavor" == "hdp" ]; then
    hostname="sandbox-hdp.hortonworks.com"`

4.	With the previous execution docker created a container with the name sandbox-hdp so when I tried to fix the network alias I got another error

      *docker: Error response from daemon: Conflict. The container name "/sandbox-hdp" is already in use by container "09a100c04aba47fbd8ee1ea99c9a932efdf62713d5eb0964bb9d3a4652757123". You have to remove (or rename) that container to be able to reuse that name.*

    I removed the previous container and run again the shell file to create a new one
  		`docker rm $CONTAINERID
  		sh docker-deploy-{HDPversion}.sh`


After this I followed the instruction to set up the sanbox hostname and to change the access password to it:
    https://es.hortonworks.com/tutorial/learning-the-ropes-of-the-hortonworks-sandbox/


## HDFS & Hive on Spark

To Install Java 8 Oracle JDK I found the instruction here:
    https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-on-ubuntu-18-04  

Then I Installed SBT using the commands from the web page of SBT
	`echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
	sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
  sudo apt-get update
  sudo apt-get install sbt`

To build the application I downloaded Intellij following the instruction here:
    https://www.jetbrains.com/help/idea/install-and-set-up-product.html

# Building Scala Application

After reading the example on Hortonworks webpage I started to search how to copy the CVS files to the sandbox, it was not an easy task because I never used Scala nor Spark before, there were many examples around but not all were completely understandable.

1.  I had troubles setting up the library dependencies and resolvers in *build.sbt* file because I wasn’t including the Spark version. I still need to investigate a little more about setting up this because it gave me some troubles when I tried to use others.

2. I spent a lot of time understanding and setting up the variables for the *SparkSession* so they pointed to the sandbox and not to the local machine.

  `val sparkSession = SparkSession.builder
    .master("local")
    .appName("TableToHive")
    .config("spark.sql.warehouse.dir",$warehouseLocation)
    .config("hive.metastore.warehouse.dir", $metastore)
    .enableHiveSupport()
    .getOrCreate()`

3. At the begging, when I run some examples, I couldn’t understand why I did not see the tables I was creating on the sandbox but I was getting results from them, after reading some post posts I understood that I was not deploying against the sandbox so I read how to generate the Jar File and after running it with the sandbox terminal I understood a little more about the variables of the *SparkSession*.

4. I started then with the application, uploading the CVS files was not a difficult task, I configured de *FilesSystem* in a variable to point to the sandbox and used the *copyFromLocalFile()* function to do it. In the code,  for this exercise I hardcoded the path from their directory and the path to the HDFS file system on the sandbox to get access to them.

   `val hadoopConf = new Configuration()
   hadoopConf.set("fs.defaultFS", $SanboxHostname)
   val hdfs = FileSystem.get(hadoopConf)    
   hdfs.copyFromLocalFile($localPath,$remotePath) `

5. With the files in the sandbox file system I created a dataframe to read the files and write them as table on Hive

    `val dataFrame= sparkSession.read
        .option("header","true")
        .option("inferSchema", "true")
        .csv($path)
    dataFrame.write.mode(SaveMode.Overwrite).saveAsTable($tablename)`

6. Finally I create another dataframe so I can query the table with sql and store the result in it to show it

    `import sparkSession.sql
    val dfHive = sql("SELECT d.DriverId, d.Name, t.Hours_Logged, t.Miles_Logged from drivers d " +
      "INNER JOIN (SELECT DriverId, sum(hours_logged) as Hours_Logged , sum(miless_logged) as Miles_Logged " +
      "FROM timesheet GROUP BY DriverId ) t ON (d.DriverId = t.DriverId)")
    dfHive.show()`


## HBase

1.  Again, I had trouble with libraries dependencies but this time because of the resolvers. I tried then a lot of different examples but the ones I was able to run in the IDE did not create tables and when I run them in the sandbox I was getting this error.

    *Java.lang.NoClassDefFoundError: org/apache/ adoop/hbase/HbaseConfiguration*

  After reading some post, most of them advised to Add all the HBase library jars to HADOOP_CLASSPATH on hadoop-env.sh file:

    *export HADOOP_CLASSPATH="$HADOOP_CLASSPATH:$HBASE_HOME/lib/"*

  I tried that but since I don't know much about all this variables and the file system of the sandbox in docker, the things that I tried did not worked for me. I kept searching and found a way to run it passing the path of hbase Jar library as and argument with this:

    `spark-submit --class HDFS --master local --conf spark.driver.extraClassPath= $HBASE_CLASSPATH`

  It worked fine for me, but since I was testing only the HBase examples, when I started to code in the same project where I had the Hive part of the code, I got and error for multiple dependencies (can write exact error because that documentation was lost in the virtual machine I was using)

  Finally I could run the code to use Hive with some HBase example together passing as arguments the pacakage and repository for Spark-HBase Connector  

    `spark-submit --class HDFS --master local --packages com.hortonworks:shc-core:1.1.0-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ ./files_2.11-0.1.jar`

2. I started this part of the code copying the files in /data-hbase/ to the sandbox FileSystem using the same method as before. *copyFromLocalFile()*
3. Then I created a dataFrame to load in there the cvs file dangerous-driver.csv with *sparkSession.read* as before and use it in forward steps to create an HBase Table
4. I found the library Spark-HBase Connector that allowed me to create the Hbase table using a dataframe and defining a catalog as an HBase schema for the table; so I created a catalog dinamically using the dataframe Schema for further uses with other tables

  `val rowKey = "eventId"  
   val stringCatalog = createCatalogString(dataFrame.schema.toList, path.getName.substring(0,path.getName.indexOf(".")),rowKey)
   def catalog = stringCatalog.stripMargin`

  `def createCatalogString(schema: List[StructField], tableName: String, rowKey: String): String = {
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
  }`



5. And now with the catalog, I could create the HBase table like this:

    `dataFrame
      .write.options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()`

    Here I found and issue because Spark-HBase Connector gives and error with the Timestamp type, so I decided to load de CVS file without the option inferSchema and it worked for me at least for this time, for other code maybe is not the best solution.

6. Afterwars I load the extra-driver.csv data into a dataframe so I can work with it. Here I did another solution that only works for this exercise.    The new row to be added to the dangerous-driver table has the same eventId of and existing one, so if I don't change this eventId it will overwrite the existing record.

	This solution only works for this example data, because if the file contains more rows, they can overwrite existing	rows if they have same eventId. A solution could be creating a new column with a different and autoincrement ID

    `dataFrame.na.replace("eventId",Map("1"->"4"))`

Adding the write function to append the new row to the table. Notice that in here I don't use the *HBaseTableCatalog.newTable -> "5"* argument used when I created the table.

    `.write
    .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
    .format("org.apache.spark.sql.execution.datasources.hbase")
    .save()`

7. Now with the table and the new row I create a dataframe with sqlContext to be able to use sql to query the HBase table. Here I use the catalog again so the dataframe can load the data

      `var df  = sparkSession.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->catalog))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()`

8. I replace routeName  "Los Angeles to Santa Clara" for "Santa Clara to San Diego" in record 4 using filter and replace functions

    `df.filter("eventId == \"4\"").na.replace("routeName", Map("Santa Clara to San Diego"->"Los Angeles to Santa Clara"))`

    And I save the change to the table using the write function as in the step 6

9. And finally I query the dataframe using a temporary template

    `df.registerTempTable("dangerousDriver")
    var query = sparkSession.sqlContext.sql("select driverName, eventType, eventTime from dangerousDriver where routeName like \"Los Angeles to%\"")`


## Final notes

If you look at the code it seams that is a simple task, after expending all this time investigating and reading how to do it, it certainly is easier; but as I mentioned I had not hands on experience with these tools, so it was difficult, especially because each tool and the sandbox have many configurations that have to be considered. Also I had to read a lot of scala documentation on how to do things or how functions worked every time I wanted to do something.

Was a really fun experience, I got frustrated some times because I couldn't find the solution or the solution didn't worked for me,  I keep doing it not only to finish this exercise to present it to you, but because I was enjoying the challenge and wanted to find the solution.


