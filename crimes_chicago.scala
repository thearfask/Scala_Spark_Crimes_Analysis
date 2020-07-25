package SparkCore
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,unix_timestamp}

object crimes_chicago {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Crimes").master("local[*]").getOrCreate()
    val sc =  spark.sparkContext
    sc.setLogLevel("OFF")
//    val data = sc.textFile("/Users/arfaa/Downloads/Datasets/ChicagoCrimes/")
//    data.take(5).foreach(println)

    val df = spark.read
        .option("header",true)
        .option("inferschema",true)
        .csv("/Users/arfaa/Downloads/Datasets/ChicagoCrimes/")

    val df_req = df.select("Date", "Primary Type")
    val df_dt = df_req
      .withColumn("Date",unix_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss")
      .cast("timestamp"))

    val df_gb = df_dt
      .withColumn("Month",col("Date").substr(0,7))
        .withColumnRenamed("Primary Type", "Crimes")
    df_gb.createOrReplaceTempView("sql_crimes")

    val df_final = spark.sql("select Month, Crimes, count(1) count_per_crime_per_date " +
      "from sql_crimes " +
      "group by Month,Crimes " +
      "order by count_per_crime_per_date desc")

//    df_final.write.option("sep","|").option("header",true)
//        .csv("/Users/arfaa/Downloads/Datasets/ChicagoCrimes/output/Crimes")

    val df_year = df_dt.withColumn("Year",col("Date").substr(0,4))
      .withColumnRenamed("Primary Type", "Crimes")

    df_year.createOrReplaceTempView("sql_crimes_year")
    val df_year_group = spark.sql("select Year, Crimes, count(Crimes) count_crime_per_year " +
      "from sql_crimes_year " +
      "group by Year, Crimes " +
      "order by count_crime_per_year desc")

    df_year_group.show()




  }

}
