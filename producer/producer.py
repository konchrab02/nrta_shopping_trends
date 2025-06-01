import random
import faker
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import datetime
from collections import Counter

# Wczytanie danych
products = pd.read_csv('data/products.csv')
shipModes = pd.read_csv('data/shipModes.csv')
shipTypes = pd.read_csv('data/shipTypes.csv')
fakerGenerator = faker.Faker(locale='en_US')

def json_serializer(obj):
    # Jeśli obiekt ma metodę isoformat (np. datetime.datetime, datetime.date lub podobne),
    # to ją użyjemy, niezależnie od tego, czy jest instancją datetime.datetime czy innego typu.
    if hasattr(obj, 'isoformat'):
        return str(obj.isoformat())
    if isinstance(obj, (pd.Series, pd.DataFrame)):
        return obj.to_dict()
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    if hasattr(obj, 'tolist'):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def generate_biased_datetime(start_date, end_date):
    # Definiowanie przedziałów czasowych i ich wag
    time_slots = [
        (0, 13, 1),    # Noc do południa (niska waga)
        (13, 16, 7),   # 13:00-16:00 (wysoka waga)
        (16, 18, 3),   # 16:00-18:00 (niska waga)
        (18, 23, 7),   # 18:00-23:00 (wysoka waga)
        (23, 24, 2),   # 23:00-24:00 (niska waga)
    ]

    # Tworzenie listy wag
    weights = [slot[2] for slot in time_slots]

    # Wybór przedziału czasowego z odpowiednią wagą
    chosen_slot = random.choices(time_slots, weights=weights, k=1)[0]

    # Losowanie godziny w wylosowanym przedziale
    hour = random.randint(chosen_slot[0], chosen_slot[1] - 1)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    # Losowanie daty w zadanym przedziale
    delta = end_date - start_date
    random_day = random.randint(0, delta.days)
    random_date = start_date + datetime.timedelta(days=random_day)

    # Łączenie daty i godziny w jeden datetime
    final_datetime = datetime.datetime(
        random_date.year, random_date.month, random_date.day, hour, minute, second
    )

    return final_datetime

# Lista klientów bazowych (imię + nazwisko)
base_clients = [(fakerGenerator.first_name(), fakerGenerator.last_name()) for _ in range(200)]

# Funkcję, która z określonym prawdopodobieństwem wybiera klienta z bazy lub wygeneruje nowego:
def get_client(base_clients, fakerGenerator, prob_base=0.7):
    if random.random() < prob_base:
        return random.choice(base_clients)  # z bazy
    else:
        return (fakerGenerator.first_name(), fakerGenerator.last_name())  # nowy losowy


# Połączenie z brokerem Kafka z retry
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode('utf-8')
        )
        print("Połączono z brokerem Kafka")
        break
    except NoBrokersAvailable:
        print(f"({i+1}/10) Broker niedostępny, ponawiam próbę za 5 sekund...")
        time.sleep(5)

if not producer:
    print("Nie udało się połączyć z brokerem Kafka.")
    exit(1)

# Główna pętla generująca dane
while True:
    first_name, last_name = get_client(base_clients, fakerGenerator)
    productIndex = random.randint(0, len(products) - 1)
    product = products.iloc[productIndex]
    shipMode = shipModes.sample(1).iloc[0]
    shipType = shipTypes.sample(1).iloc[0]
    methodOfPayment = random.choices(['Cash on delivery', 'Card', 'Card on delivery'],
                                     weights=[0.3, 0.6, 0.1],
                                     k=1)[0]
    endDate = datetime.datetime.now()
    startDate = endDate - datetime.timedelta(days=7)

    maxOrderQuantity = int(product["Max Order Quantity"])
    warehouseQuantity = int(product["Warehouse Quantity"])

    quantity = random.randint(1, maxOrderQuantity)

    # Sprawdzenie czy wystarczy produktu
    if warehouseQuantity < quantity:
        print(f"Za mało produktu '{product['Product Name']}' w magazynie ({warehouseQuantity} < {quantity}), pomijam...")
        time.sleep(2)
        continue

    # Odjęcie zamówionej ilości z magazynu
    newWarehouseQuantity = warehouseQuantity - quantity

    # Jeżeli stan magazynu spadnie poniżej 10% Max Order Quantity, uzupełnij
    if newWarehouseQuantity < (0.1 * (maxOrderQuantity * 1000)):
        newWarehouseQuantity += maxOrderQuantity * 1000
        warehouse_data = {
            "category": str(product["Category"]),
            "sub_category": str(product["Sub-Category"]),
            "product": str(product["Product Name"]),
            "production_price": float(product["Production Price"]),
            "old_warehouse_quantity": warehouseQuantity,
            "new_warehouse_quantity": newWarehouseQuantity
        }
        producer.send("warehouse_topic", warehouse_data)
        print("Sent:", warehouse_data)
        time.sleep(2)
        print(f"Magazyn uzupełniony dla produktu '{product['Product Name']}'")

    # Aktualizacja wartości w DataFrame
    products.at[productIndex, "Warehouse Quantity"] = newWarehouseQuantity

    # Zapisz zaktualizowany plik CSV
    products.to_csv('data/products.csv', index=False)

    order_data = {
        "first_name": first_name,
        "last_name": last_name,
        "address": fakerGenerator.address(),
        "phone_number": fakerGenerator.basic_phone_number(),
        "email": fakerGenerator.email(),
        "product": str(product["Product Name"]),
        "quantity": int(quantity),
        "unit_price": float(product["Price"]),
        "production_price": float(product["Production Price"]),
        "category": str(product["Category"]),
        "sub_category": str(product["Sub-Category"]),
        "warehouse_quantity": newWarehouseQuantity,
        "ship_mode": str(shipMode["Ship Mode"]),
        "ship_mode_price": float(shipMode["Price"]),
        "ship_type": str(shipType["Ship Type"]),
        "ship_type_price": float(shipType["Price"]),
        "method_of_payment": str(methodOfPayment),
        "date_of_order": str(generate_biased_datetime(start_date=startDate, end_date=endDate).isoformat())

    }

    producer.send("orders-topic", order_data)
    print("Sent:", order_data)
    time.sleep(5)
