// This script is developed on Apache Spark v2.4.4
// The Scala version is 2.12.2
// To run the script please replace the 'relativePath' with the corresponding path in your system
// To run the script please start spark-shell on your system then the Spark context and Spark session should also be started as well with required libraries
// After starting Spark ":load <file system path to script>/spark_task.scala"

val relativePath = "<relativePath>"
val sourcecsv = spark.read.option("header", "true").option("delimiter", ",").csv(relativePath + "input_data.csv")
//sourcecsv.show()
//println(sourcecsv.count())

val sourcecsv_unique = sourcecsv.dropDuplicates()
//sourcecsv_unique.show()
//println(sourcecsv_unique.count())

val sourcecsv_filtered = sourcecsv_unique.filter("categories_all is not null")
//sourcecsv_filtered.show()
//println(sourcecsv_filtered.count())

val sourcecsv_appended2 = sourcecsv_filtered.withColumn("survey_date", concat(substring(col("reporting_month"),0,4), lit("-"), substring(col("reporting_month"),5,2)))
//sourcecsv_appended2.show()
//sourcecsv_appended2.groupBy("survey_date").count().show()

val sourcecsv_appended3 = sourcecsv_appended2.withColumn("categories_all", trim(col("categories_all")))
//sourcecsv_appended3.show()
//sourcecsv_appended3.groupBy("categories_all").count().show()

val sourcecsv_exploded = sourcecsv_appended3.withColumn("categories_all", explode(split($"categories_all", "[~]")))
//sourcecsv_exploded.show()
//println(sourcecsv_exploded.count())
//sourcecsv_exploded.groupBy("categories_all").count().show()

val mappingcsv = spark.read.option("header", "true").option("delimiter", ";").csv(relativePath + "mapping.csv")
//mappingcsv.show()
//println(mappingcsv.count())

val joined_dataframe1 = sourcecsv_exploded.join(mappingcsv, sourcecsv_exploded("categories_all") === mappingcsv("Category"))
val joined_dataframe1_filtered = joined_dataframe1.drop("Category")
//joined_dataframe1_filtered.show()
//println(joined_dataframe1_filtered.count())
//joined_dataframe1_filtered.groupBy("categories_all").count().show()
//joined_dataframe1_filtered.groupBy("Mapping").count().show()

val course_category_csv = spark.read.option("header", "true").option("delimiter", ";").csv(relativePath + "course_category.csv")
//course_category_csv.show()
//println(course_category_csv.count())

val colNames = Seq("COURSE_CAT_CD", "ACTIVE_IND", "COURSE_CAT_DESC", "VALUE", "Field 5", "Field 6", "Field 7")
val course_category_csv_renamed = course_category_csv.toDF(colNames: _*)
//course_category_csv_renamed.show()

val course_category_csv_filtered = course_category_csv_renamed.drop("ACTIVE_IND", "Field 5", "Field 6", "Field 7")
//course_category_csv_filtered.show()

val joined_dataframe2 = joined_dataframe1_filtered.join(course_category_csv_filtered, joined_dataframe1_filtered("categories_all") === course_category_csv_filtered("VALUE"))
//joined_dataframe2.show()
//println(joined_dataframe2.count())
//joined_dataframe2.groupBy("Mapping").count().show()
//joined_dataframe2.groupBy("categories_all").count().show()
//println(joined_dataframe2.filter("Mapping is not null").count())

val joined_dataframe2_filtered = joined_dataframe2.drop("categories_all", "VALUE")
//joined_dataframe2_filtered.show()

val key_category_csv = spark.read.option("header", "true").option("delimiter", ";").csv(relativePath + "key_category_cluster.csv")
//key_category_csv.show()
//println(key_category_csv.count())

val key_category_csv_filtered = key_category_csv.drop("DEP_CD", "DEP_DESC")
//key_category_csv_filtered.show()

val key_category_csv_renamed = key_category_csv_filtered.withColumnRenamed("Values", "Key_Mapping")
//key_category_csv_renamed.show()

val key_category_csv_filtered2 = key_category_csv_renamed.filter("Key_Mapping is not null")
//key_category_csv_filtered2.show()

val joined_dataframe3 = joined_dataframe2_filtered.join(key_category_csv_filtered2, joined_dataframe2_filtered("Mapping") === key_category_csv_filtered2("Key_Mapping"), "left_outer")
//joined_dataframe3.show()
//println(joined_dataframe3.count())
//joined_dataframe3.groupBy("Mapping").count().show()
//joined_dataframe3.groupBy("COURSE_CAT_DESC").count().show()
//joined_dataframe3.groupBy("KEY_CAT_CLUST_DESC").count().show()

val joined_dataframe3_filtered = joined_dataframe3.select("respondent_id", "survey_date", "COURSE_CAT_CD", "COURSE_CAT_DESC", "KEY_CAT_CLUST_CD", "KEY_CAT_CLUST_DESC").dropDuplicates()
//joined_dataframe3_filtered.show()

// partitioned file 
//joined_dataframe3_filtered.write.format("csv").save(relativePath + "output_data_partitioned")

// single file
joined_dataframe3_filtered.coalesce(1).write.format("com.databricks.spark.csv").mode("overwrite").option("header", "true").option("delimiter", ";").option("quoteMode", "true").save(relativePath + "output_data_single")