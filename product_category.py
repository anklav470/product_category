from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создание Spark сессии
spark = SparkSession.builder.getOrCreate()

# Создание датафрейма "Продукты"
products_data = [("Продукт 1",), ("Продукт 2",), ("Продукт 3",)]
products_df = spark.createDataFrame(products_data, ["Имя продукта"])

# Создание датафрейма "Категории"
categories_data = [("Категория 1", "Продукт 1"), ("Категория 1", "Продукт 2"), ("Категория 2", "Продукт 2"),
                   ("Категория 3", "Продукт 3")]
categories_df = spark.createDataFrame(categories_data, ["Имя категории", "Имя продукта"])

# Объединение датафреймов по полю "Имя продукта"
result_df = products_df.join(categories_df, on="Имя продукта", how="left")

# Удаление дубликатов
result_df = result_df.dropDuplicates()

# Вывод результата
result_df.show()
