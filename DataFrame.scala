package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {
    
    val spark= SparkSession.builder()
              .appName("spark_dataframe")
              .master("local[*]")
              .getOrCreate()

    val df= spark.read
          .option("header","true")
          .option("delimiter",";")
          .option("inferSchema","true")
          .csv("/home/sary/Téléchargements/spark-df/src/main/resources/codesPostaux.csv")
    
    df.show()

    // Quel est le schéma du fichier ? 
    df.printSchema

    //Affichez le nombre de communes.
    df.agg(countDistinct("Nom_commune")).show() //32772

    //Affichez le nombre de communes qui possèdent l’attribut Ligne_5
    println(df.where(col("Ligne_5").isNotNull).count()) // 4659

    val newdf=df.withColumn("num_departement",col("Code_postal").substr(0,2))

    //Ajoutez aux données une colonne contenant le numéro de département de la commune. 
    //Ecrivez le résultat dans un nouveau fichier CSV nommé “commune_et_departement.csv”, ayant pour colonne Code_commune_INSEE, Nom_commune, Code_postal, departement, ordonné par code postal.
    
    
    val com_et_dep = newdf.select(col("Code_commune_INSEE"),col("Nom_commune"),col("Code_postal"),col("num_departement")).orderBy(asc("Code_postal"))

    com_et_dep.write.format("csv").option("header",true).option("delimiter",",").mode("overwrite").csv("src/main/resources/commune_et_departement.csv")
    


    //Affichez les communes du département de l’Aisne.
    newdf.where(col("num_departement").equalTo("02")).show()
    
    //Quel est le département avec le plus de communes ?

    newdf.groupBy("num_departement","Code_commune_INSEE") // Département 62
      .count
      .groupBy("num_departement")
      .count
      .sort(col("count").desc)
      .limit(1)
      .show()


  }


}


