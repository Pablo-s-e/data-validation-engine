package org.yiyit.validations

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yiyit.logs.LogInserts.{insertProcessValidationLog, insertTriggerControlFlag}
import org.yiyit.connection.DbConnection.readTable
import org.yiyit.time_tracker.TimeTracker

import scala.collection.mutable.ArrayBuffer

object ReferentialIntegrityValidation {

  /**
   * Valida la integridad referencial de aquellas columnas que tengan un conjunto de valores válidos
   * predefinido.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param db_table_id_name Tupla con ID y nombre de la tabla que se va a validar.
   * @param df Dataframe la tabla que se va a validar.
   * @param sl_df Dataframe de la tabla semantic_layer. Se usa para obtener la información sobre las
   *              columnas de la tabla.
   * @param row_count Número de filas de la tabla.
   * @param spark SparkSession implícita del objeto principal App. Se utiliza para importar spark.implicits.
   * @return Boolean para comprobar si la validación se ha superado.
   */
  def validateReferentialIntegrity(db_schema: String,
                                   db_table_id_name: (String, String),
                                   df: DataFrame,
                                   sl_df: DataFrame,
                                   row_count: Int
                                  )(implicit spark: SparkSession): Boolean = {

    // Tomar el tiempo al iniciar la validación
    val time_tracker = new TimeTracker
    time_tracker.setInitialTimestamp()

    // Variables para monitorizar la ejecución
    var valid_table: Boolean = true
    val validation_id: Int = 3
    val type_validation: String = "Referential Integrity"
    var flag: Double = 1.3
    var error_msg: String = ""
    var incidences = 0
    val error_field_names = new ArrayBuffer[String]()

    // Obtener los nombres de las columnas en semantic_layer
    val sl_cols_df = sl_df
      .select("id_type_file", "field_position", "field_name", "referential_table_field_name")
      .where(col("id_type_file") === db_table_id_name._1 && col("referential_table_field_name").isNotNull)
      .orderBy("field_position")
      .toDF()

    // Comprobar que el df no esté vacío; que haya columnas para comprobar
    if (sl_cols_df.isEmpty) {
      // Si está vacío, se considera exitosa la validación
      println(s"La tabla ${db_table_id_name._2} no tiene columnas cuya integridad referencial haya que comprobar.")
      valid_table
    } else {
      // Si no está vacío:
      // Recorrer las filas de semantic_layer cuyo valor de referential_table_field_name no sea null
      // Estas son las columnas cuyos valores deben coincidir con la lista de valores en la tabla referencial
      sl_cols_df.collect().foreach { sl_row =>

        // Nombre de la columna que se está comprobando
        val col_name = sl_row.getAs[String]("field_name")
        println(s"Nombre de la columna: $col_name")

        // Lista de valores válidos de tdtemplates
        import spark.implicits._
        val valid_values_list = readTable(s"$db_schema.td_templates")
          .select(col_name)
          .as[String]
          .collect()

        // Filtrar por filas cuyos valores no se encuentren en la lista de valores válidos
        val incorrect_values: Boolean = df.select(col_name)
          .where(!col(col_name).isin(valid_values_list: _*))
          .limit(1)
          .isEmpty

        // Si hay al menos un valor no válido, cambiar valid_table a false y añadir al contador de incidencias
        if (!incorrect_values) {
          valid_table = false
          incidences += 1
          error_field_names.append(col_name)
        }
      }

      println(f"Se ha validado la integridad referencial de las columnas. " +
        f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

      // Insertar en los logs en función de al conclusión de si la tabla es válida o no:
      if (valid_table) {
        // Si es válida, insertar en los logs (trigger_control)
        insertTriggerControlFlag(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          flag = flag,
          row_count = row_count
        )
        // Devolver valid_table (true)
        valid_table
      } else {
        // Si no es válida:
        // - Se cambia flag, error_msg
        flag = 33
        error_msg = s"Error de integridad referencial. La(s) columna(s) ${error_field_names.mkString(", ")} tiene(n) valores diferentes a los preestablecidos."
        // Se inserta en los logs (trigger_control y process_validation_logs)
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
          field_name = if (error_field_names.nonEmpty) {error_field_names.mkString(", ")} else null,
          incidences = incidences,
          flag = flag
        )

        valid_table // Devolver si la tabla ha superado o no la validación
      }
    }
  }
}
