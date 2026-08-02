package org.yiyit.validations

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.yiyit.logs.LogInserts.{insertProcessValidationLog, insertTriggerControlFlag}
import org.yiyit.connection.DbConnection.{getRowCount, readTable}
import org.yiyit.time_tracker.TimeTracker

/**
 * Contiene la función de validación del header de la tabla.
 */
object TableValidation {

  /**
   * Lee el dataframe de una tabla, valida si el header de la tabla coincide con los nombres en
   * semantic_layer y renombra las columnas en el caso de que no tenga header.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param db_table_id_name Tupla con ID y nombre de la tabla que se va a validar.
   * @param fc_df Dataframe de la tabla file_configuration. Se usa para obtener el schema donde se encuentra
   *              la tabla que se va a validar.
   * @param sl_df Dataframe de la tabla semantic_layer. Se usa para obtener la información sobre las columnas
   *              de la tabla.
   * @param spark SparkSession implícita del objeto principal App. Se usa para devolver dataframes vacíos
   *              cuando hay errores y no se va a devolver el dataframe de la tabla.
   * @return (DataFrame, Boolean, Int): Dataframe de la tabla con las columnas renombradas o vacío si hay error,
   *         Boolean para comprobar si la validación se ha superado, Int que contiene el row_count de la tabla.
   */
  def validateTable(db_schema: String,
                    db_table_id_name: (String, String),
                    fc_df: DataFrame,
                    sl_df: DataFrame
                   )(implicit spark: SparkSession): (DataFrame, Boolean, Int) = {

    // Tomar el tiempo al iniciar la validación
    val time_tracker = new TimeTracker
    time_tracker.setInitialTimestamp()

    // Variables para monitorizar la ejecución
    var valid_table: Boolean = true
    val validation_id: Int = 1
    val type_validation: String = "Header"
    var flag: Double = 1
    var error_msg: String = ""

    // Coger la fila de la tabla en file_configuration con el id, nombre y schema
    val fc_table_row: Row = fc_df
      .select("id_type_file", "header", "origin_path")
      .where(col("id_type_file") === db_table_id_name._1)
      .head()

    // * No haría falta comprobar si la fila está vacía ya que el nombre de la tabla por el que se filtra
    //   se obtiene de la misma tabla file_configuration

    // Coger el schema de la base de datos en el que se encuentra la tabla y formar el nombre para queries
    val schema_name: String = fc_table_row.getAs[String]("origin_path")
    val full_table_name: String = s"$schema_name.${db_table_id_name._2}"
    var row_count = 0

    println(f"Se han obtenido los datos de file_configuration y el nº de filas a través de " +
      f"getRowCount(). (${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Leer el dataframe de la bbdd, capturando excepciones si no se lee la tabla correctamente
    try {
      // Leer el dataframe de la base de la datos
      println(s"Leyendo la tabla ${db_table_id_name._2}")
      val db_table_df: DataFrame = readTable(full_table_name)

      // Mostrar el row_count de la tabla
      row_count = getRowCount(full_table_name)
      println(s"Número de filas de la tabla ${db_table_id_name._2}: $row_count")

      // Si no ha saltado ninguna excepción se sigue con la validación y se inserta en trigger_controll
      println(f"Se ha leído la tabla ${db_table_id_name._2} correctamente y obtenido el nº de filas. " +
        f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

      // Al leer una tabla correctamente, se inserta un registro inicial en trigger_control
      insertTriggerControlFlag(
        db_schema = db_schema,
        db_table_id_name = db_table_id_name,
        flag = 1,
        row_count = row_count
      )

      // Actualizar el flag a 1.1 para indicar que se están procesando
      flag = 1.1

      // Comrobar si el valor de header es true o false
      val has_header: Boolean = fc_table_row.getAs[Boolean]("header")
      println(s"La tabla ${db_table_id_name._2} tiene header: [$has_header]")

      // Obtener los nombres de las columnas en semantic_layer
      import spark.implicits._
      val sl_col_names: Array[String] = sl_df
        .select("id_type_file", "field_position", "field_name")
        .where(col("id_type_file") === db_table_id_name._1)
        .orderBy("field_position")
        .select("field_name")
        .as[String]
        .collect()

      // Mostrar ambas listas de columnas
      println(s"Columnas de semantic_layer: ${sl_col_names.mkString(", ")}")
      println(s"Columnas en el dataframe: ${db_table_df.columns.mkString(", ")}")

      // Combprobar si tiene header, renombrar las columnas y devolver el dataframe
      if (has_header) {
        // Si tiene header, habrá que comprobar si los nombres de las columnas son correctos
        println(s"La tabla ${db_table_id_name._2} tiene header." +
          s"\nSe comprobará si los nombres de las columnas son correctos.")

        // Guardar las columnas del dataframe
        val db_table_cols: Array[String] = db_table_df.columns

        // Comprobar si los nombres de las columnas son correctos
        if (db_table_cols.sameElements(sl_col_names)) {
          // Si los nombres de la columnas son correctos se devuelve el df y valid_table (true) y se guarda en logs
          println("Tiene header y los nombres de las columnas del dataframe y de semantic layer coinciden")
          // - Se cambia flag a 1.2
          flag = 1.2
          // - Se guarda en los logs (trigger_control)
          insertTriggerControlFlag(
            db_schema = db_schema,
            db_table_id_name = db_table_id_name,
            flag = flag,
            row_count = row_count
          )
          // - Se termina la validación y se devuelve el df correcto y valid_table (true)
          (db_table_df, valid_table, row_count)
        } else {
          // Si los nombres de las columnas no son correctos:
          // - Se cambia valid_table a false, flag a 31 y el mensaje de error
          valid_table = false
          flag = 31
          error_msg = "Error - Tiene header pero los nombres de las columnas del dataframe y de semantic layer no coinciden"
          println(error_msg)
          // - Se guarda en los logs (trigger_control y process_validation_logs)
          insertTriggerControlFlag(
            db_schema = db_schema,
            db_table_id_name = db_table_id_name,
            flag = flag,
            row_count = row_count
          )
          insertProcessValidationLog(
            db_schema = db_schema,
            db_table_id_name = db_table_id_name,
            validation_id = validation_id,
            type_validation = type_validation,
            validation_msg = error_msg,
            field_name = null,
            incidences = 1,
            flag = flag
          )
          // - Se devuelve un dataframe vacío y valid_table (false)
          (spark.emptyDataFrame, valid_table, row_count)
        }
      } else {
        // Si no tiene header, se supera la validación, pero hay que renombrar las columnas
        println(s"La tabla ${db_table_id_name._2} NO tiene header." +
          s"\nSe procederá a renombrar las columnas a partir de semantic layer.")

        // Capturar la excepción en caso de que el nº de columnas del df y de semantic_layer no sea el mismo
        try {
          // Renombrar las columnas
          val renamed_df: DataFrame = db_table_df.toDF(sl_col_names: _*)

          // Si no ha saltado ninguna excepción, la validación ha sido exitosa y se registra en los logs:
          println("Columnas renombradas a partir de semantic layer")

          // - Se cambia flag a 1.2
          flag = 1.2
          // - Se guarda en los logs (trigger_control)
          insertTriggerControlFlag(
            db_schema = db_schema,
            db_table_id_name = db_table_id_name,
            flag = flag,
            row_count = row_count
          )
          // - Se termina la validación y se devuelve el df correcto y valid_table (true)
          (renamed_df, valid_table, row_count)
        } catch {
          // Si el nº de columnas no es el mismo y salta una excepción:
          case e: IllegalArgumentException =>
            // - Se cambia valid_table a false, flag a 31 y el mensaje de error
            valid_table = false
            flag = 31
            error_msg = "Error - El número de columnas del dataframe y el de semantic layer no coinciden"
            println(error_msg + ". " + e.toString)
            // - Se guarda en los logs (process_validation_logs)
            insertProcessValidationLog(
              db_schema = db_schema,
              db_table_id_name = db_table_id_name,
              validation_id = validation_id,
              type_validation = type_validation,
              validation_msg = error_msg,
              field_name = null,
              incidences = 1,
              flag = flag
            )
            // - Se termina la validación y se devuelve un df vacío y valid_table (false)
            (spark.emptyDataFrame, valid_table, row_count)
        }
      }
    } catch {
      // Si salta una excepción porque la tabla no se ha leído correctamente:
      case e: Exception =>
        // - Se cambia valid_table a false, flag a 31 y el mensaje de error
        valid_table = false
        flag = 30
        error_msg = "Error - No se ha leído la tabla correctamente"
        println(error_msg + ". " + e.toString)
        e.printStackTrace()
        // - Se guarda en los logs (trigger_control y process_validation_logs)
        insertTriggerControlFlag(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          flag = flag,
          row_count = row_count
        )
        insertProcessValidationLog(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          validation_id = validation_id,
          type_validation = type_validation,
          validation_msg = error_msg,
          field_name = null,
          incidences = 1,
          flag = flag
        )
        // - Se termina la validación y se devuelve un df vacío y valid_table (false)
        (spark.emptyDataFrame, valid_table, row_count)
    }
  }
}
