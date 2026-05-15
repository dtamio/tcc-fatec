from pyspark.sql import functions as F

caminho_arquivo = ""
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ",") \
    .load(caminho_arquivo)

df_leve = (
    df
    # Ajuste dos tipos de dados
    .withColumn("VendorID", F.col("VendorID").cast("long"))
    .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
    .withColumn("passenger_count", F.col("passenger_count").cast("long"))
    .withColumn("payment_type", F.col("payment_type").cast("long"))
    .withColumn("fare_amount", F.col("fare_amount").cast("double"))
    .withColumn("extra", F.col("extra").cast("double"))
    .withColumn("mta_tax", F.col("mta_tax").cast("double"))
    .withColumn("tip_amount", F.col("tip_amount").cast("double"))
    .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
    .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))

    # Conversão da distância
    .withColumn("trip_distance", F.regexp_replace("trip_distance", ",", ".").cast("double"))

    # Filtro de pagamento,
    .filter(F.col("payment_type").isin(1, 2))

    # Duração da viagem em minutos
    .withColumn(
        "duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    )

    # Filtro de duração
    .filter((F.col("duration_min") >= 1) & (F.col("duration_min") <= 180))

    # Distância em km
    .withColumn(
        "trip_distance_km",
        F.round(F.col("trip_distance") * 1.60934, 2)
    )

    # Velocidade média
    .withColumn(
        "avg_speed_kmh",
        F.col("trip_distance_km") / (F.col("duration_min") / 60)
    )

    # Filtro de velocidade
    .filter((F.col("avg_speed_kmh") > 0) & (F.col("avg_speed_kmh") <= 120))

    # Receita líquida
    .withColumn(
        "receita_liquida",
        F.col("total_amount") - F.col("tolls_amount") - F.col("tip_amount")
    )

    # Classificação de valor
    .withColumn(
        "trip_value_category",
        F.when(F.col("fare_amount") < 10, "Baixa")
         .when(F.col("fare_amount") <= 30, "Media")
         .otherwise("Alta")
    )

    # Classificação de distância
    .withColumn(
        "trip_km_category",
        F.when(F.col("trip_distance_km") < 5, "Curta")
         .when(F.col("trip_distance_km") <= 20, "Media")
         .otherwise("Longa")
    )
)

# Ordenação
df_ordenado = df_leve.orderBy(F.col("total_amount").desc())

# Agrupamento
df_pesado = (
    df_ordenado
    .groupBy("payment_type", "trip_km_category")
    .agg(
        F.count("*").alias("Total_Corridas"),
        F.sum("total_amount").alias("Receita_Total"),
        F.avg("avg_speed_kmh").alias("Velocidade_Media")
    )
)
calculo_pesado = sum(
    F.pow(F.col("Receita_Total") + F.lit(x), 2)
    for x in range(1, 501)
)
df_pesado = df_pesado.withColumn("Calculo_Pesado", calculo_pesado)

