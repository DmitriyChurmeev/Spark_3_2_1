import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._


object App extends Context {

  override val appName: String = "Spark_3_2_1"

  def main(args: Array[String]) = {

    val athleticShoesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("sep", ",")
      .csv("src/main/resources/athletic_shoes.csv")

    import spark.implicits._

    val athleticShoesDS = athleticShoesDF.as[Shoes]

    val filteredAthleticShoesDS = athleticShoesDS
      .transform(ds => replaceValue(ds))
      .select(
        col("item_category"),
        col("item_name"),
        coalesce(col("item_after_discount"), col("item_price")) as "item_after_discount",
        col("item_price"),
        col("percentage_solds"),
        col("item_shipping"),
        col("buyer_gender"),
        col("item_rating")
      )
      .transform(ds => replaceNull(ds))

    filteredAthleticShoesDS.show()

  }

  def replaceValue(ds: Dataset[Shoes]) = {
    ds.na
      .fill(Map(
        "item_rating" -> "0",
        "buyer_gender" -> "unknown",
        "percentage_solds" -> "-1"
      ))
      .na
      .drop(List("item_name", "item_category"))
  }

  def replaceNull(ds: DataFrame) = {
    ds.na
      .fill("n/a")
  }
}


