import paho.mqtt.client as mqtt
import random
import time
import json
import multiprocessing

# Inisialisasi IP dan Port
ipv4 = "127.0.0.1"
port = 1833

def send_sensor_bojongsoang(sensor_id):
    while True:
        # Koneksi pada mqtt server
        client = mqtt.Client(f"publisher_{sensor_id}")
        client.connect(ipv4, port)
        
        # Simulasi sensor Bojongsoang
        temperature = round(25 + (sensor_id * 1.5) + random.uniform(-1, 1), 2)

        # Pembuatan data sensor Bojongsoang
        sensor_data = {f"({time.strftime('%H:%M:%S')}) Sensor Bojongsoang": temperature}

        # Mengirim data sensor dalam format JSON ke topik "bandung/bojongsoang"
        #Proses ini merupakan implementasi dari sistem terdistribusi
        status = client.publish("bandung/bojongsoang", json.dumps(sensor_data))

        # Pengecekan apakah sensor terkirim
        if status.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"({time.strftime('%H:%M:%S')}) Pesan berhasil terkirim kepada broker untuk Sensor Bojongsoang: {temperature}")
        else:
            print("Pesan gagal terkirim kepada broker untuk Sensor Bojongsoang")

        time.sleep(10)  # Menunggu 10 detik sebelum mengirim data sensor berikutnya

# Fungsi untuk mengirim data dari sensor Dago
def send_sensor_dago(sensor_id):
    while True:
        # Koneksi pada mqtt server
        client = mqtt.Client(f"publisher_{sensor_id}")
        client.connect(ipv4, port)
        
        # Simulasi sensor Dago
        temperature = round(25 + (sensor_id * 1.5) + random.uniform(-1, 1), 2)

        # Pembuatan data sensor Dago
        sensor_data = {f"({time.strftime('%H:%M:%S')}) Sensor Dago": temperature}

        # Mengirim data sensor dalam format JSON ke topik "bandung/dago"
        #Proses ini merupakan implementasi dari sistem terdistribusi
        status = client.publish("bandung/dago", json.dumps(sensor_data))

        # Pengecekan apakah sensor terkirim
        if status.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"({time.strftime('%H:%M:%S')}) Pesan berhasil terkirim kepada broker untuk Sensor Dago: {temperature}")
        else:
            print("Pesan gagal terkirim kepada broker untuk Sensor Dago")

        time.sleep(10)  # Menunggu 10 detik sebelum mengirim data sensor berikutnya
        
# Fungsi untuk mengirim data dari sensor Arcamanik
def send_sensor_arcamanik(sensor_id):
    while True:
        # Koneksi pada mqtt server
        client = mqtt.Client(f"publisher_{sensor_id}")
        client.connect(ipv4, port)
        
        # Simulasi sensor Arcamanik
        temperature = round(25 + (sensor_id * 1.5) + random.uniform(-1, 1), 2)

        # Pembuatan data sensor Arcamanik
        sensor_data = {f"({time.strftime('%H:%M:%S')}) Sensor Arcamanik": temperature}

        # Mengirim data sensor dalam format JSON ke topik "bandung/arcamanik" 
        #Proses ini merupakan implementasi dari sistem terdistribusi
        status = client.publish("bandung/arcamanik", json.dumps(sensor_data))

        # Pengecekan apakah sensor terkirim
        if status.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"({time.strftime('%H:%M:%S')}) Pesan berhasil terkirim kepada broker untuk Sensor Arcamanik: {temperature}")
        else:
            print("Pesan gagal terkirim kepada broker untuk Sensor Arcamanik")

        time.sleep(10)  # Menunggu 10 detik sebelum mengirim data sensor berikutnya


if __name__ == "__main__":
    #Proses ini merupakan implementasi dari sistem paralel
    #Membuat list untuk menyimpan refrensi ke tiga proses
    processes = []

    #Membuat Proses Pertama untuk sensor Bojongsoang
    process1 = multiprocessing.Process(target=send_sensor_bojongsoang, args=(1,))
    processes.append(process1) #Menambahkan proses 1 ke dalam list
    process1.start() #Memulai proses

    #Membuat Proses Pertama untuk sensor Dago
    process2 = multiprocessing.Process(target=send_sensor_dago, args=(2,))
    processes.append(process2)#Menambahkan proses 2 ke dalam list
    process2.start() #Memulai proses

    #Membuat Proses Pertama untuk sensor Arcamanik
    process3 = multiprocessing.Process(target=send_sensor_arcamanik, args=(3,))
    processes.append(process3)#Menambahkan proses 3 ke dalam list
    process3.start() #Memulai proses

    # Menunggu sampai semua proses selesai
    for process in processes:
        process.join()