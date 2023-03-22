import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.rand

import scala.reflect.io._

object PenguinMain {

  def foreach_batch_function = (df: Dataset[Row], epoch_id: Long) => {

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy").evaluate(df)
    println("Tasa de aciertos con streaming: " + evaluator)

  }


  def main(args: Array[String]): Unit = {

      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder().master("local[*]").appName("PenguinMain").getOrCreate()
      import spark.implicits._

      val schema = new StructType()
        .add("species", StringType)
        .add("island", StringType)
        .add("culmen_length_mm", DoubleType)
        .add("culmen_depth_mm", DoubleType)
        .add("flipper_length_mm", DoubleType)
        .add("body_mass_g", IntegerType)
        .add("sex", StringType)

       val data = spark.read
        .schema(schema)
        .format("csv")
        .option("header", value = true)
        .load("./resources/files")
        .orderBy(rand())


      val naDeleted = data
        .na.drop()
        .dropDuplicates()



      val assembler = new VectorAssembler()
        .setInputCols(Array("culmen_length_mm", "culmen_depth_mm", "flipper_length_mm", "body_mass_g"))
        .setOutputCol("features")

      val indexer = new StringIndexer()
        .setInputCol("species")
        .setOutputCol("label")

      val tree = new DecisionTreeClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setSeed(1234)


      val pipeline = new Pipeline().setStages(Array(indexer, assembler, tree))
      val splits = naDeleted.randomSplit(Array(0.6,0.2,0.2))
      val model = pipeline.fit(splits(0))
      val pred = model.transform(splits(1))

      val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("accuracy").evaluate(pred)
      println($"Tasa de aciertos: $evaluator")


      val dataToStream = splits(2).repartition(3)
      dataToStream.write
        .format("CSV")
        .option("header",true)
        .mode("overwrite")
        .save("./resources/files/splited")

      // streaming now
      val dataStreamed = spark.readStream
        .option("header", value = true)
        .schema(schema)
        .csv("./resources/files/splited")


      val predStreamed = model.transform(dataStreamed)

      val accuracy = predStreamed
        .select("label", "prediction")
        .withColumn("acierto", $"label"===$"prediction")

      accuracy.writeStream
        .outputMode("append")
        .format("console")
        .foreachBatch(foreach_batch_function)
        .start()
        .awaitTermination()


      spark.close()

  }


}
