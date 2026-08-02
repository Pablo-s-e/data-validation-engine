package org.yiyit.connection

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.util.Properties

/**
 * Contiene las conexiones a la base de datos para leer o escribir.
 */
object DbConnection {

  /**
   * Carga las propiedades de la conexión a la base de datos desde el archivo db.properties.
   */
  private lazy val props: Properties = {
    val p = new Properties()
    val stream = getClass.getClassLoader.getResourceAsStream("db.properties")

    if (stream == null) {
      throw new RuntimeException("Could not find db.properties in classpath")
    }

    p.load(stream)
    p
  }

  /**
   * Lee una tabla de la base de datos a través de properties con Spark y devuelve su dataframe.
   *
   * @param table_name Nombre de la tabla a leer.
   * @param spark SparkSession implícita del objeto principal App. Se usa para leer la tabla con spark.read.
   * @return Dataframe de la tabla que se ha leído.
   */
  // Lectura de una tabla secuencial
  def readTable(table_name: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", props.getProperty("db.host"))
      .option("dbtable", s"$table_name")
      .option("user", props.getProperty("db.username"))
      .option("password", props.getProperty("db.password"))
      .option("driver", props.getProperty("db.driver"))
      .option("fetchsize", "50000")
      .load()
  }

  /**
   * Obtiene el número de filas de una tabla a través de una query a la base de datos.
   *
   * @param table_name Nombre de la tabla cuyo número de filas se va a contar.
   * @return Número de filas de la tabla.
   */
  def getRowCount(table_name: String): Int = {
    Class.forName(props.getProperty("db.driver"))
    val conn = java.sql.DriverManager.getConnection(
      props.getProperty("db.host"),
      props.getProperty("db.username"),
      props.getProperty("db.password")
    )

    try {
      val stmt = conn.createStatement()
      val rs = stmt.executeQuery(s"SELECT COUNT(*) FROM $table_name")
      rs.next()
      rs.getInt(1)
    } finally {
      conn.close()
    }
  }

  /**
   * Inserta filas en una tabla de la base de datos.
   *
   * @param df Dataframe de las filas que se van a insertar.
   * @param tableName Nombre de la tabla a la que se van a insertar filas.
   */
  def insertRow(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", props.getProperty("db.host"))
      .option("user", props.getProperty("db.username"))
      .option("password", props.getProperty("db.password"))
      .option("driver", props.getProperty("db.driver"))
      .option("dbtable", s"$tableName")
      .mode("append")
      .save()
  }
}

