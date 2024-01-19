from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max, regexp_replace
from pyspark.sql.types import DoubleType, StringType
import re
from textblob import TextBlob
from pyspark.sql.functions import udf

def initialize_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark

def clean_data(spark, input_csv_path):
    data = spark.read.csv(input_csv_path, header=True, inferSchema=True)
    
    # Handling null values in "Review Text" column
    data = data.na.fill({'Review Text': ''})

    clean_df = data.withColumn("Price", regexp_replace(col("Product Price"), "[₹,]", ""))
    clean_df = clean_df.withColumn("Price", clean_df["Price"].cast(DoubleType()))

    def clean_emojis(text):
        return re.sub(r'[^\w\s,]', '', text)
    
    spark.udf.register("clean_emojis_udf", clean_emojis)
    clean_df = clean_df.withColumn("Review", udf(clean_emojis)(col("Review_Text")))
    clean_df = clean_df.drop("Product Price")
    clean_df = clean_df.dropDuplicates(["Reviewer Name"])

    # Save cleaned data to CSV file
    clean_df.write.csv("cleaned_data_output.csv", header=True, mode="overwrite")
    
    return clean_df

def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment_polarity = blob.sentiment.polarity
    if sentiment_polarity > 0:
        sentiment = "Positive"
    elif sentiment_polarity < 0:
        sentiment = "Negative"
    else:
        sentiment = "Neutral"
    return sentiment

if __name__ == "__main__":
    spark = initialize_spark_session()
    input_csv_path = r"..\Dataset\outputs.csv"

    try:
        cleaned_data = clean_data(spark, input_csv_path)

        sentiment_udf = udf(analyze_sentiment, StringType())
        cleaned_data = cleaned_data.withColumn("Sentiment", sentiment_udf(col("Review_Text")))
        
        product_sentiment_counts = (
            cleaned_data.groupBy("Product Name")
            .agg(
                count(when(col("Sentiment") == "Positive", 1)).alias("Positive Count"),
                count(when(col("Sentiment") == "Negative", 1)).alias("Negative Count"),
                count(when(col("Sentiment") == "Neutral", 1)).alias("Neutral Count"),
                max(col("Price")).alias("Price")
            )
        )

        # Save or further process the results
        # product_sentiment_counts.write.csv("product_sentiment_counts_output.csv", header=True, mode="overwrite")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        spark.stop()
