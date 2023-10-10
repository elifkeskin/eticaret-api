import com.mongodb.spark.MongoSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class Application {
    public static void main(String[] args) {

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


        Dataset<Row> loadDS = sparkSession.read().format("kafka")
                .option("kafka.bootstrap.servers", "206.189.117.218:9092")
                .option("subscribe", "search-analysis-userid") //Bu topic'e abone ol deriz.
                .load();

        //  loadDS.printSchema();  |-- value: binary (nullable = true) olduğu için Cast ederiz.
        Dataset<Row> rowDataset = loadDS.selectExpr("CAST(value AS STRING)");

        //Veriler JSON formatında produce edildiği için, belirtilen şemaya göre value kolonu cast ederiz.
        Dataset<Row> valueDS = rowDataset.select(functions.from_json(rowDataset.col("value"), schema).as("jsontostructs"))
                .select("jsontostructs.*");

        //Hangi yarım saat diliminde hangi ürün aranmıştır?
        Dataset<Row> current_ts_window = valueDS.groupBy(functions.window(valueDS.col("current_ts"), "30 minute"),
                valueDS.col("search")).count();

        MongoSpark.write(current_ts_window).option("collection","TimeWindowSearch").save();

        //En çok arama yapılan 10 ürün:
       /* Dataset<Row> searchGroup = valueDS.groupBy("search").count();
        Dataset<Row> searchResult = searchGroup.sort(functions.desc("count")).limit(10);

        //Hangi kullanıcı (userid), hangi ürünü(search) kaç kez aramış?
        Dataset<Row> countDS = valueDS.groupBy("userid", "search").count();
        Dataset<Row> filterDS = countDS.filter("count>10");
        Dataset<Row> pivot = filterDS.groupBy("userid").pivot("search").count().na().fill(0);*/




        //Sonuçları MongoDB'ye kaydetmek için:
        // Mode-->overwrite--> Veriyi sürekli üzerine yazar.
        // Mode-->append--> Her günün datası aynı collection üzerinde tutulur.
       // MongoSpark.write(pivot).option("collection", "searchByUserid").mode("overwrite").save();


    }
}
