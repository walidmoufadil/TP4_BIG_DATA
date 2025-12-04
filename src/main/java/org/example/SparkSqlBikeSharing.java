package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class SparkSqlBikeSharing {
    public static void main(String[] args) {
        // Étape 0 : Initialisation de SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("TP_Spark_SQL_Bike_Sharing")
                .master("local[*]")
                .getOrCreate();

        // Étape 1 : Chargement du CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("src/main/resources/bike_sharing.csv");

        // 1.2 Afficher le schéma
        System.out.println("\n=== Schéma ===");
        df.printSchema();

        // 1.3 Premières 5 lignes
        System.out.println("\n=== 5 premières lignes ===");
        df.show(5);

        // 1.4 Nombre total de locations
        long totalRentals = df.count();
        System.out.println("\n=== Nombre total de locations ===");
        System.out.println(totalRentals);

        // Étape 2 : Convertir start_time en timestamp et extraire l'heure
        Dataset<Row> dfWithHour = df
                .withColumn("start_time", to_timestamp(col("start_time"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("start_hour", hour(col("start_time")));

        // Étape 3 : Créer une vue temporaire
        dfWithHour.createOrReplaceTempView("bike_rentals_view");

        // Étape 4 : Requêtes SQL de base
        System.out.println("\n=== Locations > 30 minutes ===");
        spark.sql("SELECT * FROM bike_rentals_view WHERE duration_minutes > 30").show();

        System.out.println("\n=== Locations depuis 'Station A' ===");
        spark.sql("SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'").show();

        System.out.println("\n=== Revenu total ===");
        spark.sql("SELECT SUM(price) AS total_revenue FROM bike_rentals_view").show();

        // Étape 5 : Agrégations
        System.out.println("\n=== Locations par station de départ ===");
        spark.sql(" SELECT start_station, COUNT(*) AS rental_count FROM bike_rentals_view GROUP BY start_station ORDER BY rental_count DESC ").show();

        System.out.println("\n=== Durée moyenne par station ===");
        spark.sql(" SELECT start_station, AVG(duration_minutes) AS avg_duration FROM bike_rentals_view GROUP BY start_station ORDER BY avg_duration DESC ").show();

        System.out.println("\n=== Station la plus populaire ===");
        spark.sql(" SELECT start_station, COUNT(*) AS rental_count FROM bike_rentals_view GROUP BY start_station ORDER BY rental_count DESC LIMIT 1 ").show();

        // Étape 6 : Analyse temporelle
        System.out.println("\n=== Locations par heure (heures de pointe) ===");
        spark.sql(" SELECT start_hour, COUNT(*) AS rentals_per_hour FROM bike_rentals_view WHERE start_hour IS NOT NULL GROUP BY start_hour ORDER BY rentals_per_hour DESC ").show(24);

        System.out.println("\n=== Station la plus populaire le matin (7–12h) ===");
        spark.sql(" SELECT start_station, COUNT(*) AS morning_rentals FROM bike_rentals_view WHERE start_hour BETWEEN 7 AND 12 GROUP BY start_station ORDER BY morning_rentals DESC LIMIT 1 ").show();

        // Étape 7 : Analyse utilisateur – créer age_group
        Dataset<Row> dfWithAgeGroup = dfWithHour.withColumn("age_group",
                when(col("age").between(18, 30), "18-30")
                        .when(col("age").between(31, 40), "31-40")
                        .when(col("age").between(41, 50), "41-50")
                        .when(col("age").geq(51), "51+")
                        .otherwise("Autre")
        );
        dfWithAgeGroup.createOrReplaceTempView("bike_rentals_view");

        System.out.println("\n=== Âge moyen ===");
        spark.sql("SELECT AVG(age) AS average_age FROM bike_rentals_view").show();

        System.out.println("\n=== Utilisateurs par genre ===");
        spark.sql("SELECT gender, COUNT(*) AS user_count FROM bike_rentals_view GROUP BY gender").show();

        System.out.println("\n=== Tranche d'âge la plus active ===");
        spark.sql("SELECT age_group, COUNT(*) AS rental_count FROM bike_rentals_view WHERE age_group != 'Autre' GROUP BY age_group ORDER BY rental_count DESC").show();

        // Fermer Spark
        spark.close();
        System.out.println("\n✅ Analyse terminée.");
    }
}
