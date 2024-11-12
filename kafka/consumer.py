from kafka import KafkaConsumer
import csv
import os
from json import loads
import time

# Kafka Consumer setup
consumer = KafkaConsumer(
    'kafka-server',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',  # Only read new messages
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Pastikan folder 'output' ada
output_folder = 'output'
os.makedirs(output_folder, exist_ok=True)

# File index untuk menghasilkan nama file dinamis
file_index = 1

# Fieldnames sesuai dengan nama kolom dari dataset Anda
fieldnames = [
    'id', 'Age', 'Accessibility', 'EdLevel', 'Employment', 'Gender', 'MentalHealth', 
    'MainBranch', 'YearsCode', 'YearsCodePro', 'Country', 'PreviousSalary', 
    'HaveWorkedWith', 'ComputerSkills', 'Employed'
]

# Nama file output pertama
current_output_file = f'{output_folder}/output{file_index}.csv'

# Inisialisasi file pertama
with open(current_output_file, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

# Counter untuk menyimpan setiap 10,000 data
batch_data = []
count = 0
batch_size = 10000
batch_limit = 3  # Batasi hingga 3 batch
batch_count = 0

# Waktu pengumpulan data batch
last_batch_time = time.time()
batch_interval = 10  # Simpan batch setiap 10 detik

# Counter untuk melacak setiap 100 data yang disimpan
saved_data_count = 0

for message in consumer:
    # Tambahkan data pesan ke batch
    batch_data.append(message.value)
    count += 1

    # Simpan data jika batch sudah penuh atau jika batas waktu batch telah tercapai
    if count >= batch_size or time.time() - last_batch_time >= batch_interval:
        # Tulis batch data ke file CSV saat ini
        with open(current_output_file, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerows(batch_data)

        # Bersihkan batch data dan reset counter
        batch_data = []
        count = 0
        last_batch_time = time.time()
        print(f"{batch_size} data tersimpan ke {current_output_file} pada {time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Ganti ke file output berikutnya jika lebih dari 1 batch
        batch_count += 1
        if batch_count >= batch_limit:
            print("Batch limit tercapai, menghentikan proses.")
            break  # Hentikan proses setelah 3 batch

        file_index += 1
        current_output_file = f'{output_folder}/output{file_index}.csv'
        
        # Buat header baru untuk file berikutnya
        with open(current_output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    # Menampilkan pesan setiap 100 data yang disimpan
    if saved_data_count % 100 == 0 and saved_data_count > 0:
        print(f"{saved_data_count} data telah tersimpan sejauh ini ke {current_output_file}")
    
    # Update counter for saved data
    saved_data_count += 1

# Menyimpan sisa data yang kurang dari 100 (jika ada)
if batch_data:
    with open(current_output_file, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writerows(batch_data)
    print(f"Sisa data tersimpan ke {current_output_file}")

