import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class StreamingApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession sparkSession=SparkSession.builder()
                .master("local")
                .config("spark.mongodb.output.uri", "mongodb://206.189.117.218/eticaret.populerurunler")
                .appName("Spark Search Analysis").getOrCreate();

        //Şema kısmı eğer hatalı olursa, dataları "null" şeklinde alır.
        StructType schema=new StructType()
                .add("search", DataTypes.StringType)
                .add("current_ts", DataTypes.StringType)
                .add("region", DataTypes.StringType)
                .add("userid", DataTypes.IntegerType);


        Dataset<Row> loadDS = sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", "206.189.117.218:9092")
                .option("subscribe", "search-analysis-stream") //Bu topic'e abone ol deriz.
                .load();

        //  loadDS.printSchema();  |-- value: binary (nullable = true) olduğu için Cast ederiz.
        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");

        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("data"))
                .select("data.*");

        Dataset<Row> maskeFilter = valueDS.filter(valueDS.col("search").equalTo("maske"));

        //Trigger--> İçerisine vereceğimiz değer kadar bekler ve ondan sonra job'ı çalıştırır.60000 ms=1 dk
        //Her 1 dk'da kaydetme işlemi yapar.
        maskeFilter.writeStream().trigger(Trigger.ProcessingTime(60000)).foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
            @Override
            public void call(Dataset<Row> rowDataset, Long aLong) throws Exception {
                MongoSpark.write(rowDataset).option("collection","SearchWithMask").mode("append").save();
            }
        }).start().awaitTermination();


    }
}
