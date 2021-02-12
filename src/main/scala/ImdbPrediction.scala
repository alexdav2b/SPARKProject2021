import imdb.ImdbLib
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ImdbPrediction extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Projet 4IABD Semestre 1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val imdbPersons = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .format("csv")
    .option("delimiter","\t")
    .load("src/main/imdb_data/name.basics.tsv")
  //imdbPersons.printSchema()

  val imdbTitles = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .format("csv")
    .option("delimiter","\t")
    .load("src/main/imdb_data/title.basics.tsv")

  val imdbPrincipals = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .format("csv")
    .option("delimiter","\t")
    .load("src/main/imdb_data/title.principals.tsv")

  val imdbRatings = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .format("csv")
    .option("delimiter","\t")
    .load("src/main/imdb_data/title.ratings.tsv")

  val imdbLib = new ImdbLib()

  /**Classement des oeuvres les mieux notées en étant les plus populaires
   */

  val coef_udf = udf(imdbLib.intervalle _)
  val imdbCoeffs = imdbRatings.withColumn("coefficient",  coef_udf(col("numVotes")))
  //imdbCoeffs.show()

  val bestRated = imdbCoeffs.selectExpr("*", "(averageRating*coefficient)/4 as popularite")
    .sort(desc("popularite"))
  println("----------------bestRated---------------")
  bestRated.show(100)

  /**Classement des oeuvres les mieux notées en étant les plus populaires
   */

  val coef_udf = udf(imdbLib.intervalle _)
  val imdbCoeffs = imdbRatings.withColumn("coefficient",  coef_udf(col("numVotes")))
  //imdbCoeffs.show()

  val bestRated = imdbCoeffs.selectExpr("*", "(averageRating*coefficient)/4 as popularite")
    .sort(desc("popularite"))
  println("----------------bestRated---------------")
  bestRated.show(100)

  /** Croisement des données pour obtenir une valeur de prédiction
   */

  //Dataframe contenant pour chaque personne les oeuvres pour lesquelles elle est connue
  val splitDF = imdbPersons.withColumn("knownForTitles", split(col("knownForTitles"), ","))
    .withColumn("primaryProfession", split(col("primaryProfession"), ","))
  val explodedDF = splitDF.select(col("*"), explode(col("knownForTitles")).as("knownForTitle"))
    .drop("knownForTitles", "birthYear", "deathYear")
  //explodedDF.show()

  //Explosion de la liste des emplois
  val explodedDF2 = explodedDF.select(col("*"), explode(col("primaryProfession")).as("primaryProfessions"))
    .drop("primaryProfession")

/*  Liste de tous les emplois possibles, ne sert qu'à filtrer plus tard
  val professions = explodedDF2.select("primaryProfessions")
    .distinct()
  //professions.show()
*/

  //Jointure entre la liste de personnes et des films, avec leur cote de popularite
  val personWithMovie = explodedDF2.join(bestRated, bestRated("tconst") === explodedDF2("knownForTitle"))
    .drop("tconst")
    .sort("nconst")
  println("----------------personWithMovie---------------")
  personWithMovie.show()

  //Jointure entre les personnes principales d'un film et les films principaux d'une personne pour ne garder que les +++ importants, pareil pour le join, please don't blame me
  val importantAndSuccessful = personWithMovie.join(imdbPrincipals,
    personWithMovie("knownForTitle") === imdbPrincipals("tconst")
    && personWithMovie("nconst") === imdbPrincipals("nconst")
    && personWithMovie("primaryProfessions") === imdbPrincipals("category"))
    .drop("job", "tconst", "primaryProfessions")
    .drop(imdbPrincipals("nconst"))
    .sort(desc("popularite"))
  println("----------------importantAndSuccessful---------------")
  importantAndSuccessful.show()

  //Calcul de la moyenne de la popularité des oeuvres de chaque personne
  val theSTARS = importantAndSuccessful.groupBy("nconst", "primaryName")
    .agg(avg("popularite").as("moyennePop"))
    .sort(desc("moyennePop"))
  println("----------------theStars---------------")
  theSTARS.show()

  /** Dataframes de prédiction
   */

  //On croise les rôles importants avec leurs acteurs respectifs
  val predict1 = imdbPrincipals.join(imdbPersons,
    imdbPersons("nconst") === imdbPrincipals("nconst")
  )
    .drop(imdbPersons("nconst"))
    .drop("knownForTitles", "birthYear", "deathYear", "job", "primaryProfession")
    .sort("tconst")
  println("----------------predict1---------------")
  predict1.show()

  val predict2 = predict1.join(theSTARS, "nconst")
    .sort("tconst")
  println("----------------predict2---------------")
  predict2.show()

  //Calcul de la moyenne des notes des films d'un acteur et de l'importance de son rôle dans un film donné
  val ourPrediction1 = predict2
    .selectExpr("tconst", "nconst", "(moyennePop*(1/ordering)) as ValeurAjoutee_Acteur")
    .sort(desc("ValeurAjoutee_Acteur"))
  println("----------------ValeurAjoutee---------------")
  ourPrediction1.show()

  //Aggrégation des valeurs ajoutées des personnes afin de prédire le "succès" d'un film
  val ourPrediction2 = ourPrediction1.groupBy("tconst")
    .agg(avg("ValeurAjoutee_Acteur").as("prediction"))
    .sort(desc("prediction"))
  //println("----------------prediction---------------")
  //ourPrediction2.show()

  //DF avec les noms des films
  val prettyDF = ourPrediction2.join(imdbTitles, "tconst")
    .drop("titleType", "originalTitle","isAdult","startYear","endYear","runtimeMinutes","genres")
    .sort(desc("prediction"))
  println("----------------final Dataframe---------------")
  prettyDF.show()
}