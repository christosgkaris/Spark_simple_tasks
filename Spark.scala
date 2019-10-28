import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/*
Narrow vs Wide transformations
- Narrow transformations do not move data around the network
- Wide transformations move data around the network
*/


/*
Function to fix the "depth" value
Sources:
	- https://stackoverflow.com/questions/9542126/how-to-
		find-if-a-scala-string-is-parseable-as-a-double-or-not
	- https://www.scala-lang.org/old/node/255.html
	- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
*/
def depthToDouble(s: String): Double = {
	try 
		s.replaceAll("m", "").toDouble
	catch {
		case e: Exception => -1.0
	}
}

def newDepth = udf(depthToDouble _)



/* Question 1: Read each measurement folder into a separate data frame */
/* Direction */
val dfDirTemp = spark.read.
	option("header", true).
	option("inferSchema", true).
	option("delimiter", ";").
	csv("smhi/direction")

/* Flux */
val dfFluxTemp = spark.read.
	option("header", true).
	option("inferSchema", true).
	option("delimiter", ";").
	csv("smhi/flux")

/* Salinity */
val dfSalTemp = spark.read.
	option("header", true).
	option("inferSchema", true).
	option("delimiter", ";").
	csv("smhi/salinity")

/* Temperature */
val dfTemTemp = spark.read.
	option("header", true).
	option("inferSchema", true).
	option("delimiter", ";").
	csv("smhi/temperature")


/* Question 2: Provide schemas such that each feature has the expected type */
/* Sources: 
	- https://stackoverflow.com/questions/29383107/how-to-change-column-
		types-in-spark-sqls-dataframe 
	- https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-udfs.html
*/
/* Direction */
val dfDir = dfDirTemp.
	select("station", "latitude", "longitude", "ts", "value", "quality", "depth").
	withColumn("station", dfDirTemp("station").cast(IntegerType)).
	withColumn("latitude", dfDirTemp("latitude").cast(DoubleType)).
	withColumn("longitude", dfDirTemp("longitude").cast(DoubleType)).
	withColumn("ts", dfDirTemp("ts").cast(TimestampType)).
	withColumn("value", dfDirTemp("value").cast(DoubleType)).
	withColumn("quality", dfDirTemp("quality").cast(StringType)).
	withColumn("depth", newDepth(dfDirTemp("depth")).cast(DoubleType))

/* Flux */
val dfFlux = dfFluxTemp.
	select("station", "latitude", "longitude", "ts", "value", "quality", "depth").
	withColumn("station", dfFluxTemp("station").cast(IntegerType)).
	withColumn("latitude", dfFluxTemp("latitude").cast(DoubleType)).
	withColumn("longitude", dfFluxTemp("longitude").cast(DoubleType)).
	withColumn("ts", dfFluxTemp("ts").cast(TimestampType)).
	withColumn("value", dfFluxTemp("value").cast(DoubleType)).
	withColumn("quality", dfFluxTemp("quality").cast(StringType)).
	withColumn("depth", newDepth(dfFluxTemp("depth")).cast(DoubleType))

/* Salinity */
val dfSal = dfSalTemp.
	select("station", "latitude", "longitude", "ts", "value", "quality", "depth").
	withColumn("station", dfSalTemp("station").cast(IntegerType)).
	withColumn("latitude", dfSalTemp("latitude").cast(DoubleType)).
	withColumn("longitude", dfSalTemp("longitude").cast(DoubleType)).
	withColumn("ts", dfSalTemp("ts").cast(TimestampType)).
	withColumn("value", dfSalTemp("value").cast(DoubleType)).
	withColumn("quality", dfSalTemp("quality").cast(StringType)).
	withColumn("depth", newDepth(dfSalTemp("depth")).cast(DoubleType))

/* Temperature */
val dfTem = dfTemTemp.
	select("station", "latitude", "longitude", "ts", "value", "quality", "depth").
	withColumn("station", dfTemTemp("station").cast(IntegerType)).
	withColumn("latitude", dfTemTemp("latitude").cast(DoubleType)).
	withColumn("longitude", dfTemTemp("longitude").cast(DoubleType)).
	withColumn("ts", dfTemTemp("ts").cast(TimestampType)).
	withColumn("value", dfTemTemp("value").cast(DoubleType)).
	withColumn("quality", dfTemTemp("quality").cast(StringType)).
	withColumn("depth", newDepth(dfTemTemp("depth")).cast(DoubleType))


/* Question 3: Join all data frames into one */
/* Sources: 
	- https://stackoverflow.com/questions/47820078/how-to-join-specific-column-of-
		dataframe-with-another-in-scala-spark
	- https://stackoverflow.com/questions/35592917/renaming-column-names-of-a-
		dataframe-in-spark-scala

	Justification: The four data frames have the same seven columns. What seperates 
	the data is the type of value (direction value, flux value, salinity value, 
	temperature value) and the quality of the value. So the proper way to join 
	them, is to use the other five columns as keys (as for a given combination of 
	these five keys, there can be values for direction and/or flux and/or salinity 
	and/or temperature and the respective qualities). The type of join is outer, 
	because we want to keep all the elements from all four data frames. 
	Source: 
		- https://www.w3schools.com/sql/sql_join.asp
*/
// join: wide transformation
val df = dfDir.withColumnRenamed("value", "dir_value").
				withColumnRenamed("quality", "dir_quality").
			join(dfFlux.withColumnRenamed("value", "flux_value").	
						withColumnRenamed("quality", "flux_quality"), 
				Seq("station", "latitude", "longitude", "ts", "depth"),
				"outer").
 			join(dfSal.withColumnRenamed("value", "sal_value").
						withColumnRenamed("quality", "sal_quality"),
				Seq("station", "latitude", "longitude", "ts", "depth"),
				"outer").
			join(dfTem.withColumnRenamed("value", "tem_value").
						withColumnRenamed("quality", "tem_quality"),
				Seq("station", "latitude", "longitude", "ts", "depth"),
				"outer").
			persist()


/* Question 4: Answer the following questions */
/* How many records are there? Answer: 7329777 */
/* To get the number of records in a data frame, I apply count() */
//val numOfRecs = df.count()

/* How many stations are there? Answer: 200 */
/* To get the number of stations, first I use select to get them. Then 
I use groupBy to group them and count to get the number of records for 
each station. Then I apply count to get the number of records, which is the 
number of stations */
// groupBy: wide transformation
/*val numOfStations = df.
	select("station").
	groupBy("station").
	count().
	count()
*/

/* How many recordings has each station made? */
/* To get the number of records per station, first I use select to get 
them. Then I use groupBy to group them and count to get the number of 
records for each station. Then I use orderBy to order by station */
// groupBy, orderBy: wide transformations
/*val numOfRecsPerStation = df.
	select("station").
	groupBy("station").
	count().
	orderBy("station").
	show(/*200*/)
*/


/* Question 5: Perform the following aggregations */
/* Mean temperature, salinity or flux (choose at least one) for all 
	stations (not per station) over all years */
/* To get the mean salinity over all years, first I select the timestamps 
and the salinity values. Then I use groupBy to group them by year. Then I 
use agg to calculate the avg of the sal_value. Then I use orderBy to order 
the result by year */
// groupBy, orderBy: wide transformations
/*val q5_1 = df.
	select("ts", "sal_value").
	groupBy(year($"ts")).
	agg(avg("sal_value")).
	orderBy(year($"ts")).
	show(/*147*/)
*/

/* Mean temperature, salinity or flux (choose at least one) for all 
	stations (not per station) for each month of each year, since year 2000 */
/* To get the mean salinity for each month of each year, since year 2000, first 
I select the timestamps and the salinity values. Then I use filter to keep the 
values from 2000 and later. Then I use groupBy to group the salinity values by 
year and by month. Then I use agg to calculate the avg of the sal_value. Then I 
use orderBy to order the result by year and by month */
// groupBy, orderBy: wide transformations
// filter: narrow transformation
/*val q5_2 = df.
	select("ts", "sal_value").
	filter(year($"ts") > 1999).
	groupBy(year($"ts"), month($"ts")).
	agg(avg("sal_value")).
	orderBy(year($"ts"), month($"ts")).
	show(/*229*/)
*/

/* Mean temperature, salinity or flux (choose at least one) for all 
	stations (not per station) for windows of 15 days, since year 2010 */
/* To get the mean salinity for windows of 15 days, since year 2010, first 
I select the timestamps and the salinity values. Then I use filter to keep 
the values from 2010-01-16 and later. Then I use groupBy to group the salinity 
values by window of 15 days. Then I use agg to calculate the avg of the 
sal_value. Then I use orderBy to order the result by time window */
// groupBy, orderBy: wide transformations
// filter: narrow transformation
/*val q5_3 = df.
	select("ts", "sal_value").
	filter(year($"ts") >= 2010 && month($"ts") >= 1 && dayofmonth($"ts") >= 16).
	groupBy(window($"ts", "15 days")).
	agg(avg("sal_value")).
	orderBy("window").
	show(/*221, false*/)
*/

/* Mean temperature, salinity or flux (choose at least one) for all 
	stations (not per station) for windows of 15 days with sliding windows 
	of 3 days, since year 2010 */
/* To get the mean salinity for windows of 15 days with sliding windows of 3 
days, since year 2010, first I select the timestamps and the salinity values. 
Then I filter them, to keep the values from 2010-01-16 and later. Then I use 
groupBy to group the salinity values by window of 15 days, with a sliding 
window of 3 days. Then I use agg to calculate the avg of the sal_value. Then 
I use orderBy to order the result by time window */
// groupBy, orderBy: wide transformations
// filter: narrow transformation
/*val q5_4 = df.
	select("ts", "sal_value").
	filter(year($"ts") >= 2010 && month($"ts") >= 1 && dayofmonth($"ts") >= 16).
	groupBy(window($"ts", "15 days", "3 days")).
	agg(avg("sal_value")).
	orderBy("window").	
	show(/*false*/)
*/


/* Question 6: Save the resulting data frame from point 3 as parquet */
//df.write.parquet("my_parquet")
