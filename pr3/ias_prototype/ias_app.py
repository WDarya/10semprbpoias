import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import random
import tkinter as tk
from tkinter import ttk, messagebox

# -------------------------- ГЕНЕРАЦИЯ ТЕСТОВЫХ ДАННЫХ --------------------------
def generate_test_data():
    """Генерация тестовых CSV-файлов при отсутствии данных"""
    os.makedirs("data", exist_ok=True)

    # Товары (15 штук)
    products = pd.DataFrame({
        "product_id": range(1, 16),
        "product_name": [f"Товар_{i}" for i in range(1, 16)],
        "category": random.choices(["электроника", "одежда", "продукты"], k=15),
        "supplier": random.choices(["Поставщик_A", "Поставщик_B", "Поставщик_C"], k=15)
    })
    products.to_csv("data/products.csv", index=False)

    # Магазины (6 штук)
    stores = pd.DataFrame({
        "store_id": range(1, 7),
        "city": random.choices(["Москва", "СПб", "Казань"], k=6),
        "district": random.choices(["Центр", "Север", "Юг"], k=6),
        "store_type": random.choices(["гипермаркет", "супермаркет", "минимаркет"], k=6)
    })
    stores.to_csv("data/stores.csv", index=False)

    # Продажи (1200 записей)
    dates = pd.date_range("2023-01-01", "2024-12-31", freq="D")
    sales_records = []
    for _ in range(1200):
        sales_records.append({
            "date": random.choice(dates).strftime("%Y-%m-%d"),
            "product_id": random.randint(1, 15),
            "store_id": random.randint(1, 6),
            "quantity": random.randint(1, 10),
            "price": round(random.uniform(100, 5000), 2)
        })
    sales = pd.DataFrame(sales_records)
    sales.to_csv("data/sales.csv", index=False)

    print("Тестовые данные сгенерированы.")


# -------------------------- ETL PROCESSOR --------------------------
class ETLProcessor:
    """Класс для извлечения, преобразования и загрузки данных"""
    
    def __init__(self, db_path="ias.db"):
        self.db_path = db_path
        self.conn = None
        self.sales = None
        self.products = None
        self.stores = None

    def extract(self):
        """Извлечение данных из CSV-файлов"""
        self.sales = pd.read_csv("data/sales.csv")
        self.products = pd.read_csv("data/products.csv")
        self.stores = pd.read_csv("data/stores.csv")
        print("Данные извлечены.")

    def transform(self):
        """Преобразование и очистка данных"""
        # 1. Удаление пропусков
        self.sales.dropna(subset=["date", "product_id", "store_id", "quantity", "price"], inplace=True)
        self.products.dropna(subset=["product_id", "product_name"], inplace=True)
        self.stores.dropna(subset=["store_id", "city"], inplace=True)

        # 2. Вычисление выручки
        self.sales["revenue"] = self.sales["quantity"] * self.sales["price"]

        # 3. Работа с датами
        self.sales["date"] = pd.to_datetime(self.sales["date"])
        self.sales["year"] = self.sales["date"].dt.year
        self.sales["month"] = self.sales["date"].dt.month
        self.sales["quarter"] = self.sales["date"].dt.quarter

        # 4. Генерация customer_id (для подсчёта уникальных покупателей)
        self.sales["customer_id"] = [random.randint(1, 200) for _ in range(len(self.sales))]

        # 5. Проверка ссылочной целостности
        valid_products = set(self.products["product_id"])
        valid_stores = set(self.stores["store_id"])
        self.sales = self.sales[self.sales["product_id"].isin(valid_products)]
        self.sales = self.sales[self.sales["store_id"].isin(valid_stores)]

        print("Данные преобразованы.")

    def load(self):
        """Загрузка данных в SQLite"""
        self.conn = sqlite3.connect(self.db_path)
        self.sales.to_sql("fact_sales", self.conn, if_exists="replace", index=False)
        self.products.to_sql("dim_product", self.conn, if_exists="replace", index=False)
        self.stores.to_sql("dim_store", self.conn, if_exists="replace", index=False)
        print("Данные загружены в хранилище.")
        self.conn.close()

    def run(self):
        """Запуск полного ETL-процесса"""
        self.extract()
        self.transform()
        self.load()


# -------------------------- DATA WAREHOUSE --------------------------
class DataWarehouse:
    """Управление хранилищем данных"""
    
    def __init__(self, db_path="ias.db"):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Подключение к базе данных"""
        self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def check_integrity(self):
        """Проверка целостности хранилища"""
        self.conn = sqlite3.connect(self.db_path)
        tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", self.conn)
        required = {"fact_sales", "dim_product", "dim_store"}
        result = required.issubset(set(tables["name"]))
        self.conn.close()
        return result

    def close(self):
        if self.conn:
            self.conn.close()


# -------------------------- DATA MART --------------------------
class DataMart:
    """Построение витрины данных"""
    
    def __init__(self, db_path="ias.db"):
        self.db_path = db_path
        self.conn = None

    def build(self):
        """Построение витрины sales_mart с транзакцией"""
        self.conn = sqlite3.connect(self.db_path)
        query = """
        CREATE TABLE sales_mart AS
        SELECT
            f.year,
            f.quarter,
            f.month,
            p.category,
            p.supplier,
            s.city,
            s.store_type,
            SUM(f.quantity) AS total_quantity,
            SUM(f.revenue) AS total_revenue,
            AVG(f.revenue) AS avg_check,
            COUNT(DISTINCT f.customer_id) AS unique_customers,
            COUNT(*) AS transaction_count
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        JOIN dim_store s ON f.store_id = s.store_id
        GROUP BY f.year, f.quarter, f.month, p.category, p.supplier, s.city, s.store_type
        """
        try:
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("DROP TABLE IF EXISTS sales_mart")
            self.conn.execute(query)
            self.conn.commit()
            print("Витрина данных построена.")
        except Exception as e:
            self.conn.rollback()
            print(f"Ошибка при построении витрины: {e}")
            raise
        finally:
            self.conn.close()

    def get_data(self):
        """Получение данных из витрины"""
        self.conn = sqlite3.connect(self.db_path)
        data = pd.read_sql("SELECT * FROM sales_mart", self.conn)
        self.conn.close()
        return data


# -------------------------- ANALYTICS ENGINE --------------------------
class AnalyticsEngine:
    """Реализация аналитических методов"""
    
    def __init__(self, mart_data: pd.DataFrame):
        self.data = mart_data

    def abc_analysis(self):
        """ABC-анализ по выручке (группы A, B, C)"""
        product_rev = self.data.groupby("category")["total_revenue"].sum().sort_values(ascending=False)
        cumsum = product_rev.cumsum() / product_rev.sum()
        
        def abc_group(val):
            if val <= 0.8:
                return "A"
            elif val <= 0.95:
                return "B"
            else:
                return "C"
        
        product_abc = cumsum.apply(abc_group)
        
        # Визуализация
        plt.figure(figsize=(10, 6))
        colors = {'A': 'gold', 'B': 'silver', 'C': '#CD7F32'}
        bar_colors = [colors[product_abc[idx]] for idx in product_rev.index]
        product_rev.plot(kind="bar", color=bar_colors)
        plt.title("ABC-анализ по категориям (выручка)", fontsize=14)
        plt.xlabel("Категория")
        plt.ylabel("Выручка")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()
        
        # Вывод результатов
        result = pd.DataFrame({"Выручка": product_rev, "Накопленная доля": cumsum, "Группа": product_abc})
        print("\n=== ABC-анализ ===\n", result)
        return product_abc

    def xyz_analysis(self):
        """XYZ-анализ по стабильности спроса (коэффициент вариации)"""
        monthly = self.data.groupby(["category", "month"])["total_quantity"].sum().unstack(fill_value=0)
        cv = monthly.std(axis=1) / monthly.mean(axis=1)
        
        def xyz_group(cv_val):
            if cv_val <= 0.1:
                return "X"
            elif cv_val <= 0.25:
                return "Y"
            else:
                return "Z"
        
        result = cv.apply(xyz_group)
        print("\n=== XYZ-анализ ===\n", result)
        return result

    def abc_xyz_matrix(self):
        """Совмещённая матрица ABC × XYZ"""
        abc = self.abc_analysis()
        xyz = self.xyz_analysis()
        matrix = pd.DataFrame({"ABC": abc, "XYZ": xyz})
        matrix["ABC_XYZ"] = matrix["ABC"] + matrix["XYZ"]
        print("\n=== ABC-XYZ матрица ===\n", matrix)
        
        # Тепловая карта
        cross = pd.crosstab(matrix["ABC"], matrix["XYZ"])
        print("\n=== Матрица сопряжённости ===\n", cross)
        
        fig, ax = plt.subplots(figsize=(8, 6))
        im = ax.imshow(cross.values, cmap='YlOrRd', aspect='auto')
        ax.set_xticks(range(len(cross.columns)))
        ax.set_yticks(range(len(cross.index)))
        ax.set_xticklabels(cross.columns)
        ax.set_yticklabels(cross.index)
        plt.colorbar(im, ax=ax, label='Количество категорий')
        plt.title("ABC-XYZ матрица", fontsize=14)
        plt.xlabel("XYZ-группа")
        plt.ylabel("ABC-группа")
        
        for i in range(len(cross.index)):
            for j in range(len(cross.columns)):
                text = ax.text(j, i, cross.iloc[i, j], ha="center", va="center", color="black")
        plt.tight_layout()
        plt.show()

    def monthly_revenue_for_category(self, category):
        """Динамика выручки по месяцам для выбранной категории"""
        cat_data = self.data[self.data["category"] == category]
        monthly = cat_data.groupby("month")["total_revenue"].sum()
        
        plt.figure(figsize=(12, 5))
        plt.plot(monthly.index, monthly.values, marker='o', linewidth=2, markersize=8)
        plt.title(f"Динамика выручки по месяцам: {category}", fontsize=14)
        plt.ylabel("Выручка")
        plt.xlabel("Месяц")
        plt.grid(True, alpha=0.3)
        plt.xticks(range(1, 13))
        plt.tight_layout()
        plt.show()


# -------------------------- REPORTS --------------------------
class ReportGenerator:
    """Генерация аналитических отчётов"""
    
    def __init__(self, db_path="ias.db"):
        self.db_path = db_path

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def last_quarter_revenue_by_city(self):
        """Отчёт 1: Выручка за последний квартал по городам"""
        conn = self._get_connection()
        query = """
        SELECT city, SUM(total_revenue) as revenue
        FROM sales_mart
        WHERE year = (SELECT MAX(year) FROM sales_mart)
          AND quarter = (SELECT MAX(quarter) FROM sales_mart 
                         WHERE year = (SELECT MAX(year) FROM sales_mart))
        GROUP BY city
        ORDER BY revenue DESC;
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result

    def top10_products_by_revenue(self):
        """Отчёт 2: Топ-10 товаров по выручке"""
        conn = self._get_connection()
        query = """
        SELECT p.product_name, SUM(f.revenue) as revenue
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.product_name
        ORDER BY revenue DESC
        LIMIT 10;
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result

    def top5_stores_avg_check(self):
        """Отчёт 3: Топ-5 магазинов по среднему чеку"""
        conn = self._get_connection()
        query = """
        SELECT s.city, s.store_type, ROUND(AVG(f.revenue), 2) as avg_check
        FROM fact_sales f
        JOIN dim_store s ON f.store_id = s.store_id
        GROUP BY s.store_id
        ORDER BY avg_check DESC
        LIMIT 5;
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result

    def supplier_report(self):
        """Отчёт 4: Отчёт по поставщикам (выручка и доля)"""
        conn = self._get_connection()
        query = """
        SELECT p.supplier, SUM(f.revenue) as revenue,
               ROUND(100.0 * SUM(f.revenue) / (SELECT SUM(revenue) FROM fact_sales), 2) as share_pct
        FROM fact_sales f
        JOIN dim_product p ON f.product_id = p.product_id
        GROUP BY p.supplier
        ORDER BY revenue DESC;
        """
        result = pd.read_sql(query, conn)
        conn.close()
        return result


# -------------------------- GUI (TKINTER) --------------------------
class IASAapp:
    """Графический интерфейс пользователя"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Информационно-аналитическая система (продажи)")
        self.root.geometry("1000x700")
        self.db_path = "ias.db"
        self.conn = None
        self.mart_data = None

        # Создание меню
        menubar = tk.Menu(root)
        root.config(menu=menubar)

        # Меню Файл
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Файл", menu=file_menu)
        file_menu.add_command(label="1. Загрузить данные (ETL)", command=self.run_etl)
        file_menu.add_command(label="2. Построить витрину", command=self.build_mart)
        file_menu.add_separator()
        file_menu.add_command(label="Проверить целостность", command=self.check_integrity)
        file_menu.add_separator()
        file_menu.add_command(label="Выход", command=root.quit)

        # Меню Аналитика
        analytics_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Аналитика", menu=analytics_menu)
        analytics_menu.add_command(label="ABC-анализ", command=self.run_abc)
        analytics_menu.add_command(label="XYZ-анализ", command=self.run_xyz)
        analytics_menu.add_command(label="ABC-XYZ матрица", command=self.run_abc_xyz)
        analytics_menu.add_command(label="Динамика по категории", command=self.show_category_dialog)

        # Меню Отчёты
        report_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Отчёты", menu=report_menu)
        report_menu.add_command(label="Выручка за последний квартал по городам", command=self.show_report1)
        report_menu.add_command(label="Топ-10 товаров по выручке", command=self.show_report2)
        report_menu.add_command(label="Топ-5 магазинов по среднему чеку", command=self.show_report3)
        report_menu.add_command(label="Отчёт по поставщикам", command=self.show_report4)

        # Меню Справка
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Справка", menu=help_menu)
        help_menu.add_command(label="О программе", command=self.show_about)
        help_menu.add_command(label="Руководство пользователя", command=self.show_manual)

        # Основной фрейм
        mainframe = ttk.Frame(root, padding="10")
        mainframe.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        mainframe.columnconfigure(0, weight=1)
        mainframe.rowconfigure(0, weight=1)

        # Таблица для отображения отчётов
        self.tree = ttk.Treeview(mainframe)
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        scrollbar_y = ttk.Scrollbar(mainframe, orient=tk.VERTICAL, command=self.tree.yview)
        scrollbar_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        scrollbar_x = ttk.Scrollbar(mainframe, orient=tk.HORIZONTAL, command=self.tree.xview)
        scrollbar_x.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        self.tree.configure(yscrollcommand=scrollbar_y.set, xscrollcommand=scrollbar_x.set)

        # Строка статуса
        self.status = ttk.Label(root, text="Готово. Выполните ETL → Витрина → Аналитика", 
                                relief=tk.SUNKEN, anchor=tk.W)
        self.status.grid(row=2, column=0, sticky=(tk.W, tk.E))

    def run_etl(self):
        """Запуск ETL-процесса"""
        try:
            self.status.config(text="Выполняется ETL...")
            self.root.update()
            etl = ETLProcessor(self.db_path)
            etl.run()
            self.conn = sqlite3.connect(self.db_path)
            self.status.config(text="✅ ETL выполнен успешно")
            messagebox.showinfo("Успех", "Данные загружены и преобразованы")
        except Exception as e:
            self.status.config(text=f"❌ Ошибка: {str(e)}")
            messagebox.showerror("Ошибка", str(e))

    def build_mart(self):
        """Построение витрины данных"""
        try:
            if not os.path.exists(self.db_path):
                messagebox.showwarning("Нет БД", "Сначала выполните ETL (Файл → Загрузить данные)")
                return
            self.status.config(text="Построение витрины...")
            self.root.update()
            dm = DataMart(self.db_path)
            dm.build()
            self.mart_data = dm.get_data()
            self.status.config(text="✅ Витрина построена")
            messagebox.showinfo("Успех", "Витрина данных создана")
        except Exception as e:
            self.status.config(text=f"❌ Ошибка: {str(e)}")
            messagebox.showerror("Ошибка", str(e))

    def check_integrity(self):
        """Проверка целостности хранилища"""
        dw = DataWarehouse(self.db_path)
        if dw.check_integrity():
            messagebox.showinfo("Проверка целостности", "✅ Все таблицы присутствуют. Целостность не нарушена.")
        else:
            messagebox.showerror("Проверка целостности", "❌ Отсутствуют необходимые таблицы. Выполните ETL.")

    def run_abc(self):
        if self.mart_data is None:
            messagebox.showwarning("Нет данных", "Сначала постройте витрину (Файл → Построить витрину)")
            return
        ae = AnalyticsEngine(self.mart_data)
        ae.abc_analysis()

    def run_xyz(self):
        if self.mart_data is None:
            messagebox.showwarning("Нет данных", "Сначала постройте витрину")
            return
        ae = AnalyticsEngine(self.mart_data)
        ae.xyz_analysis()

    def run_abc_xyz(self):
        if self.mart_data is None:
            messagebox.showwarning("Нет данных", "Сначала постройте витрину")
            return
        ae = AnalyticsEngine(self.mart_data)
        ae.abc_xyz_matrix()

    def show_category_dialog(self):
        if self.mart_data is None:
            messagebox.showwarning("Нет данных", "Сначала постройте витрину")
            return
        categories = self.mart_data["category"].unique()
        win = tk.Toplevel(self.root)
        win.title("Выбор категории")
        win.geometry("300x150")
        ttk.Label(win, text="Выберите категорию:").pack(pady=10)
        cb = ttk.Combobox(win, values=list(categories), width=20)
        cb.pack(pady=5)
        def on_ok():
            if cb.get():
                ae = AnalyticsEngine(self.mart_data)
                ae.monthly_revenue_for_category(cb.get())
                win.destroy()
            else:
                messagebox.showwarning("Ошибка", "Выберите категорию")
        ttk.Button(win, text="Показать график", command=on_ok).pack(pady=10)

    def show_report(self, df, title):
        if df is None or df.empty:
            messagebox.showinfo("Нет данных", "Отчёт пуст")
            return
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.tree["columns"] = list(df.columns)
        self.tree["show"] = "headings"
        for col in df.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150)
        for _, row in df.iterrows():
            self.tree.insert("", tk.END, values=list(row))
        self.status.config(text=f"✓ {title}")

    def show_report1(self):
        if not os.path.exists(self.db_path):
            messagebox.showwarning("Нет БД", "Выполните ETL (Файл → Загрузить данные)")
            return
        rg = ReportGenerator(self.db_path)
        df = rg.last_quarter_revenue_by_city()
        self.show_report(df, "Выручка за последний квартал по городам")

    def show_report2(self):
        if not os.path.exists(self.db_path):
            messagebox.showwarning("Нет БД", "Выполните ETL")
            return
        rg = ReportGenerator(self.db_path)
        df = rg.top10_products_by_revenue()
        self.show_report(df, "Топ-10 товаров по выручке")

    def show_report3(self):
        if not os.path.exists(self.db_path):
            messagebox.showwarning("Нет БД", "Выполните ETL")
            return
        rg = ReportGenerator(self.db_path)
        df = rg.top5_stores_avg_check()
        self.show_report(df, "Топ-5 магазинов по среднему чеку")

    def show_report4(self):
        if not os.path.exists(self.db_path):
            messagebox.showwarning("Нет БД", "Выполните ETL")
            return
        rg = ReportGenerator(self.db_path)
        df = rg.supplier_report()
        self.show_report(df, "Отчёт по поставщикам")

    def show_about(self):
        messagebox.showinfo("О программе", 
            "Информационно-аналитическая система для розничных продаж\n\n"
            "Разработано в рамках практической работы №3\n"
            "Дисциплина: Проектирование ИАС\n\n"
            "Функции:\n"
            "• ETL-процесс (CSV → SQLite)\n"
            "• Витрина данных (Data Mart)\n"
            "• ABC/XYZ анализ\n"
            "• Генерация отчётов\n\n"
            "Автор: Белая Дарья Алексеевна\n"
            "Группа: БИСО-01-21\n"
            "Дата: 2026")

    def show_manual(self):
        messagebox.showinfo("Руководство пользователя", 
            "ПОРЯДОК РАБОТЫ С ПРОГРАММОЙ:\n\n"
            "1. Файл → Загрузить данные (ETL)\n"
            "   - Загружает данные из CSV в БД\n"
            "   - Выполняет очистку и преобразования\n\n"
            "2. Файл → Построить витрину\n"
            "   - Создаёт агрегированную витрину данных\n\n"
            "3. Аналитика → ABC/XYZ анализ\n"
            "   - Выполняет аналитические расчёты\n"
            "   - Показывает графики и таблицы\n\n"
            "4. Отчёты → Выбрать нужный отчёт\n"
            "   - Данные отображаются в таблице\n\n"
            "Для повторного анализа после новых данных\n"
            "повторите шаги 1-2.")


# -------------------------- MAIN --------------------------
if __name__ == "__main__":
    # Генерация тестовых данных при первом запуске
    if not os.path.exists("data/sales.csv"):
        print("Генерация тестовых данных...")
        generate_test_data()
        print("Тестовые данные созданы в папке 'data'")
    
    # Запуск GUI
    root = tk.Tk()
    app = IASAapp(root)
    root.mainloop()