from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import os
import time

# Inisialisasi SparkSession
spark = SparkSession.builder \
    .appName("EmploymentPredictionRandomForest") \
    .getOrCreate()

current_dir = os.path.dirname(os.path.abspath(__file__))
output_folder_path = os.path.join(current_dir, "../kafka/output")  # Folder output CSV dari Kafka
model_save_path = os.path.join(current_dir, "models")  # Folder untuk menyimpan model yang dilatih

# Fungsi untuk menambahkan label klasifikasi berdasarkan prediksi
def add_labels_from_prediction(df, prediction_col="prediction"):
    df = df.withColumn(
        "employment_label",
        when(col(prediction_col) == 1, "Employed")  # Cluster 1: Employed
        .when(col(prediction_col) == 0, "Unemployed")  # Cluster 0: Unemployed
        .otherwise("Other")  # Cluster lainnya
    )
    return df

# Fungsi untuk memuat dan memproses data setiap batch
def load_and_preprocess_data(batch_file_path):
    df = spark.read.csv(batch_file_path, header=True, inferSchema=True)
    print(f"Memproses file: {batch_file_path}")
    df.show(20)

    # Preprocessing kolom 'Age' untuk menjadi nilai numerik
    df = df.withColumn(
        "Age_numeric",
        when(col("Age") == "<35", 0)
        .when(col("Age") == ">35", 1)
        .otherwise(0)  # Gantilah dengan nilai default yang sesuai, misalnya 0 jika null
    )

    # Menangani kolom yang memiliki string kategori dan mengubahnya menjadi format numerik menggunakan StringIndexer
    categorical_columns = ['Gender', 'MainBranch', 'Country', 'HaveWorkedWith']
    indexers = [StringIndexer(inputCol=col, outputCol=col + "_index", handleInvalid='skip').fit(df) for col in categorical_columns]
    
    # Mengaplikasikan string indexing pada kolom kategori
    for indexer in indexers:
        df = indexer.transform(df)

    # Menghapus kolom kategori yang sudah diindex dan 'Age' yang asli
    df = df.drop(*categorical_columns)
    df = df.drop("Age")

    # Menghapus baris dengan nilai null pada kolom fitur
    df = df.dropna(subset=["Age_numeric", "YearsCode", "YearsCodePro", "PreviousSalary", "ComputerSkills"])

    # Menyiapkan fitur untuk model
    feature_columns = [
        'Age_numeric', 'YearsCode', 'YearsCodePro', 'PreviousSalary', 'ComputerSkills', 
        'Gender_index', 'MainBranch_index', 'Country_index', 'HaveWorkedWith_index'
    ]
    
    # Menyusun fitur menggunakan VectorAssembler
    assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
    df = assembler.transform(df)
    df = df.select('features', 'Employed')  # Pastikan hanya kolom fitur dan target yang dipilih
    return df

# Fungsi untuk melatih model Random Forest dan menyimpan hasil model
def train_and_save_rf_model(df, model_name):
    # Menghitung bobot untuk masing-masing kelas
    class_weights = df.groupBy("Employed").count().withColumnRenamed("count", "class_count")
    total_count = df.count()
    class_weights = class_weights.withColumn("weight", total_count / (2 * col("class_count")))
    
    # Menggabungkan data dengan bobot
    df_with_weights = df.join(class_weights, on="Employed", how="left")
    
    # Membuat model RandomForestClassifier dengan maxBins yang ditingkatkan dan labelCol yang tepat
    rf = RandomForestClassifier(featuresCol='features', labelCol='Employed', weightCol="weight", numTrees=10, maxBins=10000)

    # Melatih model
    model = rf.fit(df_with_weights)

    # Menggunakan model untuk prediksi pada data
    predictions = model.transform(df_with_weights)

    # Menambahkan label berdasarkan prediksi
    predictions_with_labels = add_labels_from_prediction(predictions)

    # Menyimpan model Random Forest
    model_dir = os.path.join(model_save_path, model_name)
    model.write().overwrite().save(model_dir)

    # Menampilkan hasil prediksi
    predictions_with_labels.show(20)

    # Evaluasi menggunakan MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(labelCol="Employed", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)

    print(f"Model Random Forest disimpan di {model_save_path}/{model_name}")
    print(f"Accuracy: {accuracy:.2f}")

# Fungsi untuk memproses semua batch
def process_batches():
    batch_files = sorted(os.listdir(output_folder_path))  # Mengurutkan file secara alfabet
    batch_count = 0
    
    for batch_file in batch_files:
        if batch_file.endswith('.csv'):
            batch_file_path = os.path.join(output_folder_path, batch_file)
            df = load_and_preprocess_data(batch_file_path)
            
            # Menentukan nama model berdasarkan urutan batch
            model_name = f"rf_employment_model_{batch_count + 1}"
            train_and_save_rf_model(df, model_name)
            
            batch_count += 1
            print(f"Batch {batch_count} diproses dan model {model_name} telah dilatih")
            
            time.sleep(5)  # Jeda antara proses batch

# Program utama untuk memulai pemrosesan
if __name__ == "__main__":
    process_batches()
    spark.stop()