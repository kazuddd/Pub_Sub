import paho.mqtt.client as mqtt
import json
import time

# Dictionary untuk menyimpan data sensor dari tiap proses
sensor_data = {}

# Counter untuk memantau jumlah pesan yang diterima
message_counter = 0

# Function untuk menghitung rerata dari data sensor
def calculate_average(data):
    valid_temperatures = [value for value in data.values()]
    avg_temperature = sum(valid_temperatures) / len(valid_temperatures)
    return avg_temperature

# Function untuk memproses pesan yang diterima
def on_message(client, userdata, msg):
    global sensor_data, message_counter, max_data

    message = msg.payload.decode()
    try:
        # Convert pesan JSON menjadi dictionary
        sensor_reading = json.loads(message)

        # Update sensor_data dengan bacaan sensor terbaru
        sensor_data.update(sensor_reading)

        # Meningkatkan counter untuk setiap pesan yang diterima
        message_counter += 1

        # Jika telah menerima sebanyak subscribe
        if message_counter == max_data:
            rerata = calculate_average(sensor_data)

            # Print semua data sensor yang diterima
            for sensor, value in sensor_data.items():
                print(f"({time.strftime('%H:%M:%S')}) {sensor}: {value} °C")
            
            print(f"Rata-rata suhu dari sensor yang terpilih: {rerata:.2f} °C")
            print("=" * 30)

            # Reset counter dan dictionary untuk pesan berikutnya
            message_counter = 0
            sensor_data = {}

    except json.JSONDecodeError as e:
        print(f"Error: JSON Decode Error - {e}")
    except Exception as e:
        print(f"Error: {e}")

# Function untuk pengecekan status connect saat connect
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Anda berhasil terhubung !!!");print("=" * 30)
    else:
        print("Gagal terhubung, Kode error: ", rc)


if __name__ == "__main__":
    nama = input("Masukkan Nama: ")
    client = mqtt.Client(nama)
    
    # Register fungsi callback untuk menerima pesan
    client.on_message = on_message
    
    # Register fungsi connect untuk pengecekan koneksi
    client.on_connect = on_connect
    
    # Connect ke MQTT broker
    client.connect("127.0.0.1", 1833)

    # Menu pemilihan topik
    print("Menu:")
    print("1. Sensor Bojongsoang")
    print("2. Sensor Dago")
    print("3. Sensor Arcamanik")
    print("4. Sensor Bojongsoang & Dago")
    print("5. Sensor Bojongsoang & Arcamanik")
    print("6. Sensor Dago and Arcamanik")
    print("7. Sensor Bojongsoang, Dago, & Arcamanik")
    print("0. Exit")
    
    choice = (input("Masukkan Pilihan Sensor: "))
    
    while True:
        if choice == "1":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan pada topik Bojongsoang
            client.subscribe("bandung/bojongsoang")
            max_data = 1
            break
        elif choice == "2":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Dago
            client.subscribe("bandung/dago")
            max_data = 1
            break
        elif choice == "3":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Arcamanik
            client.subscribe("bandung/arcamanik")
            max_data = 1
            break
        elif choice == "4":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Bojongsoang & Dago
            client.subscribe("bandung/bojongsoang")
            client.subscribe("bandung/dago")
            max_data = 2
            break
        elif choice == "5":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Bojongsoang & Arcamanik
            client.subscribe("bandung/bojongsoang")
            client.subscribe("bandung/arcamanik")
            max_data = 2
            break
        elif choice == "6":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Dago & Arcamanik
            client.subscribe("bandung/dago")
            client.subscribe("bandung/arcamanik")
            max_data = 2
            break
        elif choice == "7":
            #Proses ini merupakan implementasi dari sistem terdistribusi dari sisi Subscriber yang berlangganan topik Bojongsoang, Dago & Arcamanik
            client.subscribe("bandung/bojongsoang")
            client.subscribe("bandung/dago")
            client.subscribe("bandung/arcamanik")
            max_data = 3
            break
        elif choice == "0":
            client.disconnect() # Disconnect jika opsi keluar terpilih
            break
        else:
            choice = (input("Pilihan invalid, silahkan input ulang: ")) # Input ulang jika pilihan invalid
            
    print("=" * 30)
    # Mulai loop untuk menerima pesan
    client.loop_forever()