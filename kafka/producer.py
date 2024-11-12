from kafka import KafkaProducer
from json import dumps
import csv
import random
import time

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    linger_ms=5,  # Kurangi waktu batch menjadi 5ms untuk pengiriman lebih cepat
    batch_size=65536  # Tingkatkan ukuran batch menjadi 64 KB untuk pengiriman batch lebih besar
)

dataset = '../dataset/clean_dataset.csv'

with open(dataset, 'r') as f:
    csv_reader = csv.DictReader(f)
    for row in csv_reader:
        # Kirim data ke Kafka
        producer.send('kafka-server', value=row)
        
        # Variasikan waktu delay untuk simulasi skenario tugas yang berbeda (Bisa dihapus atau diperkecil)
        delay = random.uniform(0.001, 0.01)  # Delay antara 1ms hingga 10ms
        time.sleep(delay)
        
        # Menampilkan pesan dengan data yang dikirimkan
        print(f"Data dikirim: {row}")
        print(f"Delay: {delay:.4f} detik")

    # Pastikan semua pesan dikirim sebelum menutup producer
    producer.flush()
