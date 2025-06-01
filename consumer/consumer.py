from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import pandas as pd
import os
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Ścieżki do plików
orders_path = './final_data/final_orders.csv'
warehouse_path = './final_data/warehouse_updates.csv'


# Funkcja tworząca pusty plik, jeśli nie istnieje
def ensure_file(path, columns):
    if not os.path.exists(path):
        pd.DataFrame(columns=columns).to_csv(path, index=False)


ensure_file(orders_path, ["first_name", "last_name", "product", "category", "sub_category", "quantity", "unit_price", "production_price", "method_of_payment", "ship_mode", "ship_type", "order_datetime"])

ensure_file(warehouse_path, ["category", "sub_category", "product", "production_price", "old_warehouse_quantity", "new_warehouse_quantity"])


# Funkcja do połączenia z brokerem
def create_consumer(topic_name):
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers='kafka:9092',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id=f'{topic_name}-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"Połączono z brokerem Kafka jako konsument ({topic_name}).")
            return consumer
        except NoBrokersAvailable:
            print(f"({i+1}/10) Broker niedostępny, ponawiam próbę za 5 sekund...")
            time.sleep(5)
    return None


# Konsument zamówień
orders_consumer = create_consumer("orders-topic")
warehouse_consumer = create_consumer("warehouse_topic")

if not orders_consumer or not warehouse_consumer:
    print("Nie udało się połączyć z brokerem Kafka.")
    exit(1)


# Funkcja przetwarzająca dane zamówień
def process_order(data):
    print("Zamówienie:", data)
    # Wczytaj istniejące dane zamówień
    df_existing = pd.read_csv(orders_path)
    # Dodaj nowe zamówienie
    new_entry = pd.DataFrame([{
        "first_name": data["first_name"],
        "last_name": data["last_name"],
        "product": data["product"],
        "category": data["category"],
        "sub_category": data["sub_category"],
        "quantity": data["quantity"],
        "unit_price": data["unit_price"],
        "production_price": data["production_price"],
        "method_of_payment": data["method_of_payment"],
        "ship_mode": data["ship_mode"],
        "ship_type": data["ship_type"],
        "order_datetime": data["date_of_order"],
    }])
    df_updated = pd.concat([df_existing, new_entry], ignore_index=True)
    df_updated.to_csv(orders_path, index=False)

def update_analytics():
    try:
        df_orders = pd.read_csv(orders_path, parse_dates=["order_datetime"])
        df_orders["order_datetime"] = pd.to_datetime(df_orders["order_datetime"], errors="coerce")


        # Połączenie z Google Sheets (raz)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name("fleet-ivy-459215-b3-2a2e9d3df84b.json", scope)
        client = gspread.authorize(creds)
        dashboard = client.open("DashboardData")

        # Bufor arkuszy
        worksheets = {
            name: dashboard.worksheet(name)
            for name in [
                "sales_by_product",
                "orders_over_time",
                "orders_by_hour",
                "revenue_by_day",
                "revenue_by_product",
                "avg_order_value",
                "client_stats",
                "sales_heatmap",
                "profit_by_product_category",
                "orders_by_ship_mode",
                "orders_by_ship_type",
                "avg_order_value_by_payment",
                "top_express_products",
                "revenue_by_sub_category",
                "warehouse_summary"
            ]
        }

        # Sprzedaż wg produktu (ilość sztuk sprzedanych i przychód z nich)
        df_orders["revenue"] = df_orders["unit_price"] * df_orders["quantity"]
        sales_summary = df_orders.groupby("product").agg(
            total_quantity=("quantity", "sum"),
            total_revenue=("revenue", "sum"),
            unit_price=("unit_price", "first")
        ).reset_index()
        sales_summary.to_csv("./final_data/sales_by_product.csv", index=False)

        worksheets["sales_by_product"].update(
            values=[sales_summary.columns.tolist()] + sales_summary.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Liczba zamówień dziennie
        df_orders["order_date"] = df_orders["order_datetime"].dt.date
        orders_over_time = df_orders.groupby("order_date")["order_datetime"].count().reset_index()
        orders_over_time.columns = ["date", "order_count"]
        orders_over_time["date"] = orders_over_time["date"].astype(str)
        orders_over_time.to_csv("./final_data/orders_over_time.csv", index=False)

        worksheets["orders_over_time"].update(
            values=[orders_over_time.columns.tolist()] + orders_over_time.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Liczba zamówień wg godziny
        df_orders["hour"] = pd.to_datetime(df_orders["order_datetime"]).dt.hour
        orders_by_hour = df_orders.groupby("hour")["order_datetime"].count().reset_index()
        orders_by_hour.columns = ["hour", "order_count"]

        # Dodanie brakujących godziny (0–23)
        all_hours = pd.DataFrame({'hour': range(24)})
        orders_by_hour = pd.merge(all_hours, orders_by_hour, on="hour", how="left")
        orders_by_hour["order_count"] = orders_by_hour["order_count"].fillna(0).astype(int)

        orders_by_hour.to_csv("final_data/orders_by_hour.csv", index=False)

        worksheets["orders_by_hour"].update(
            values=[orders_by_hour.columns.tolist()] + orders_by_hour.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Heatmapa: liczba zamówień i przychód na godzinę w danym dniu i dniu tygodnia
        df_orders["revenue"] = df_orders["unit_price"] * df_orders["quantity"]
        df_orders["order_date"] = df_orders["order_datetime"].dt.date

        df_orders["date"] = df_orders["order_datetime"].dt.date.astype(str)
        df_orders["weekday"] = df_orders["order_datetime"].dt.day_name()
        df_orders["hour"] = df_orders["order_datetime"].dt.hour

        sales_heatmap = df_orders.groupby(["date", "weekday", "hour"]).agg(
            order_count=("order_datetime", "count"),
            revenue=("revenue", "sum")
        ).reset_index()

        sales_heatmap["revenue"] = sales_heatmap["revenue"].round(2)

        sales_heatmap.to_csv("final_data/sales_heatmap.csv", index=False)
        worksheets["sales_heatmap"].update(
            values=[sales_heatmap.columns.tolist()] + sales_heatmap.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Przychód dzienny
        revenue_by_day = df_orders.groupby("order_date")["revenue"].sum().reset_index()
        revenue_by_day["order_date"] = revenue_by_day["order_date"].astype(str)
        revenue_by_day.columns = ["date", "total_revenue"]
        revenue_by_day.to_csv("final_data/revenue_by_day.csv", index=False)

        worksheets["revenue_by_day"].update(
            values=[revenue_by_day.columns.tolist()] + revenue_by_day.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Przychód wg produktu
        revenue_by_product = df_orders.groupby("product")["revenue"].sum().reset_index()
        revenue_by_product.columns = ["product", "total_revenue"]
        revenue_by_product.to_csv("final_data/revenue_by_product.csv", index=False)

        worksheets["revenue_by_product"].update(
            values=[revenue_by_product.columns.tolist()] + revenue_by_product.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Średnia wartość zamówienia per dzień
        avg_order = df_orders.groupby(df_orders["order_datetime"].dt.date)["revenue"].mean().reset_index()
        avg_order.columns = ["date", "avg_order_value"]
        avg_order["date"] = avg_order["date"].astype(str)
        avg_order.to_csv("final_data/avg_order_value.csv", index=False)

        worksheets["avg_order_value"].update(
            values=[avg_order.columns.tolist()] + avg_order.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Ststystyki klientów
        client_orders = df_orders.groupby(["first_name", "last_name"]).agg({
            "quantity": "sum", # ile produktów zakupił klient
            "revenue": "sum", # przychód od tego klienta
            "order_datetime": "count"  # liczba zamówień danego klienta
        }).reset_index()

        client_orders.columns = ["first_name", "last_name", "total_quantity", "total_revenue", "order_count"]
        client_orders.to_csv("final_data/client_stats.csv", index=False)

        worksheets["client_stats"].update(
            values=[client_orders.columns.tolist()] + client_orders.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Analiza zyskowności sprzednaych produktów
        df_orders["total_cost"] = df_orders["production_price"] * df_orders["quantity"]
        df_orders["total_revenue"] = df_orders["unit_price"] * df_orders["quantity"]
        profit_summary = df_orders.groupby(["category", "sub_category", "product"]).agg(
            total_cost=("total_cost", "sum"),
            total_revenue=("total_revenue", "sum")
        ).reset_index()

        profit_summary["profit"] = profit_summary["total_revenue"] - profit_summary["total_cost"]
        profit_summary = profit_summary.round(2)
        profit_summary.to_csv("final_data/profit_by_product_category.csv", index=False)

        worksheets["profit_by_product_category"] = dashboard.worksheet("profit_by_product_category")
        worksheets["profit_by_product_category"].update(
            values=[profit_summary.columns.tolist()] + profit_summary.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Udział i liczba zamówień wg ship_mode
        ship_mode_summary = df_orders["ship_mode"].value_counts(normalize=False).reset_index()
        ship_mode_summary.columns = ["ship_mode", "order_count"]
        ship_mode_summary["percentage"] = (
                    ship_mode_summary["order_count"] / ship_mode_summary["order_count"].sum() * 100).round(2)

        ship_mode_summary.to_csv("final_data/orders_by_ship_mode.csv", index=False)

        worksheets["orders_by_ship_mode"] = dashboard.worksheet("orders_by_ship_mode")
        worksheets["orders_by_ship_mode"].update(
            values=[ship_mode_summary.columns.tolist()] + ship_mode_summary.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Udział i liczba zamówień wg ship_type
        ship_type_summary = df_orders["ship_type"].value_counts(normalize=False).reset_index()
        ship_type_summary.columns = ["ship_type", "order_count"]
        ship_type_summary["percentage"] = (
                    ship_type_summary["order_count"] / ship_type_summary["order_count"].sum() * 100).round(2)

        ship_type_summary.to_csv("final_data/orders_by_ship_type.csv", index=False)

        worksheets["orders_by_ship_type"] = dashboard.worksheet("orders_by_ship_type")
        worksheets["orders_by_ship_type"].update(
            values=[ship_type_summary.columns.tolist()] + ship_type_summary.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Średnia wartość zamówienia wg metody płatności
        avg_value_by_payment = df_orders.groupby("method_of_payment")["revenue"].mean().reset_index()
        avg_value_by_payment.columns = ["method_of_payment", "avg_order_value"]
        avg_value_by_payment["avg_order_value"] = avg_value_by_payment["avg_order_value"].round(2)

        avg_value_by_payment.to_csv("final_data/avg_order_value_by_payment.csv", index=False)

        worksheets["avg_order_value_by_payment"] = dashboard.worksheet("avg_order_value_by_payment")
        worksheets["avg_order_value_by_payment"].update(
            values=[avg_value_by_payment.columns.tolist()] + avg_value_by_payment.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        #Najczęściej zamawiane produkty z dostawą ekspresową
        express_orders = df_orders[df_orders["ship_mode"].str.lower() == "same day"]
        top_express_products = express_orders["product"].value_counts().head(10).reset_index()
        top_express_products.columns = ["product", "express_order_count"]

        top_express_products.to_csv("final_data/top_express_products.csv", index=False)

        worksheets["top_express_products"] = dashboard.worksheet("top_express_products")
        worksheets["top_express_products"].update(
            values=[top_express_products.columns.tolist()] + top_express_products.values.tolist(),
            range_name="A1"
        )
        time.sleep(2)

        # Revenue per Category & Sub-Category
        revenue_by_sub_category = df_orders.groupby(["category", "sub_category"])["revenue"].sum().reset_index()
        revenue_by_sub_category.columns = ["category", "sub_category", "total_revenue"]
        revenue_by_sub_category["total_revenue"] = revenue_by_sub_category["total_revenue"].round(2)

        revenue_by_sub_category.to_csv("final_data/revenue_by_sub_category.csv", index=False)

        worksheets["revenue_by_sub_category"] = dashboard.worksheet("revenue_by_sub_category")
        worksheets["revenue_by_sub_category"].update(
            values=[revenue_by_sub_category.columns.tolist()] + revenue_by_sub_category.values.tolist(),
            range_name="A1"
        )
        time.sleep(5)

        # Podsumowanie magazynu
        df_warehouse = pd.read_csv(warehouse_path)
        summary = df_warehouse.groupby(["category", "sub_category", "product"]).agg(
            total_added=("new_warehouse_quantity", "sum"),
            last_production_price=("production_price", "last")
        ).reset_index()
        summary["total_added"] = summary["total_added"].astype(int)

        summary.to_csv("final_data/warehouse_summary.csv", index=False)

        if "warehouse_summary" in worksheets:
            worksheets["warehouse_summary"].update(
                values=[summary.columns.tolist()] + summary.values.tolist(),
                range_name="A1"
            )
        time.sleep(2)

    except Exception as e:
        print("Błąd w update_analytics():", e)

def update_warehouse_summary():
    try:
        df = pd.read_csv(warehouse_path)

        # Oblicz najnowszy stan magazynowy dla każdego produktu
        df_latest = df.groupby(["category", "sub_category", "product"]).last().reset_index()
        df_latest["current_stock"] = df_latest["new_warehouse_quantity"]

        # Dodaj kolumnę informującą o niskim stanie magazynowym
        df_latest["low_stock"] = df_latest["current_stock"] < 100

        df_latest = df_latest[["category", "sub_category", "product", "production_price", "current_stock", "low_stock"]]
        df_latest.to_csv("final_data/warehouse_summary.csv", index=False)
        print("✅ Zaktualizowano warehouse_summary.csv")

    except Exception as e:
        print("❌ Błąd w update_warehouse_summary():", e)


# Funkcja przetwarzająca dane magazynowe
def process_warehouse(data):
    print("Aktualizacja magazynu:", data)
    df_existing = pd.read_csv(warehouse_path)
    new_entry = pd.DataFrame([{
        "category": data["category"],
        "sub_category": data["sub_category"],
        "product": data["product"],
        "production_price": data["production_price"],
        "old_warehouse_quantity": data["old_warehouse_quantity"],
        "new_warehouse_quantity": data["new_warehouse_quantity"]
    }])
    df_updated = pd.concat([df_existing, new_entry], ignore_index=True)
    df_updated.to_csv(warehouse_path, index=False)


# To jest wersja bez czekania, poniżej wersja z czekaniem z przesyłem do API bo przekracza to limit
# Odbieranie danych z obu konsumentów (naprzemiennie)
#while True:
    #for message in orders_consumer.poll(timeout_ms=1000).values():
        #for record in message:
          #process_order(record.value)

    #for message in warehouse_consumer.poll(timeout_ms=1000).values():
        #process_warehouse(record.value)
        #for record in message:
            #pass
    #time.sleep(1)


# Bufor do kontrolowania częstotliwości analiz
last_update_time = time.time()
UPDATE_INTERVAL = 30  # aktualizacja Google Sheets co 30 sekund

while True:
    for message in orders_consumer.poll(timeout_ms=1000).values():
        for record in message:
            process_order(record.value)

    for message in warehouse_consumer.poll(timeout_ms=1000).values():
        for record in message:
            process_warehouse(record.value)

    # Uruchomianie update_analysis co 30 sekund
    now = time.time()
    if now - last_update_time > UPDATE_INTERVAL:
        print("▶ Aktualizacja arkuszy Google Sheets...")
        update_analytics()
        update_warehouse_summary()
        last_update_time = now

    time.sleep(1)
