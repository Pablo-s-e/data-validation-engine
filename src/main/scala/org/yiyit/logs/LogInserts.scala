package org.yiyit.logs

import org.apache.spark.sql.functions.max
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yiyit.connection.DbConnection.{insertRow, readTable}

import java.sql.{Date, Timestamp}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}

/**
 * Contiene las funciones para insertar los logs en las tablas trigger_control y process_validation_logs.
 */
object LogInserts {

  /**
   * Inserta una fila como log en la tabla trigger_control cada vez que se empieza y termina de validar.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param db_table_id_name Tupla con ID y nombre de la tabla que se va a validar.
   * @param flag Número de flag que se va a insertar. Representa las etapas que la tabla ha superado
   *             con éxito en un momento concreto de la ejecución y si ha habido algún error.
   * @param row_count Número de filas de la tabla.
   * @param spark SparkSession implícita del objeto principal App. Se usa para escribir en una tabla
   *              con spark.write.
   */
  def insertTriggerControlFlag(db_schema: String,
                               db_table_id_name: (String, String),
                               flag: Double,
                               row_count: Int
                              )(implicit spark: SparkSession): Unit = {

    // Importar implicits para poder utilizar .toDF en Seq()
    import spark.implicits._

    // Crear la fila que se insertará en trigger_control
    val trigger_control_row: DataFrame = Seq(
       (db_table_id_name._1,
        db_table_id_name._2,
        "PRE",
        "yiyit_bigdata",
        Date.valueOf(LocalDate.now()),
        Timestamp.valueOf(LocalDateTime.now()),
        flag,
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toLong,
        row_count)
    ).toDF(
        "id_type_file",
        "file_name",
        "environment",
        "source",
        "date_load",
        "tst_trigger_control",
        "flag",
        "timestamp_load",
        "row_count"
    )

    // Insertar la fila
    insertRow(trigger_control_row, s"$db_schema.trigger_control")
  }

  /**
   * Inserta una fila como log en la tabla process_validation_logs en caso de que haya errores.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param db_table_id_name Tupla con ID y nombre de la tabla que se va a validar.
   * @param validation_id ID de la validación que la tabla no ha superado.
   * @param type_validation Tipo de validación que la tabla no ha superado.
   * @param validation_msg Mensaje de error. Ayuda a determinar en qué ha fallado una validación.
   * @param field_name Nombre del campo en el que ha fallado una validación.
   * @param incidences Número de incidencias encontradas en la validación.
   * @param flag Número de flag que se va a insertar. Representa las etapas que la tabla ha superado
   *             con éxito en un momento concreto de la ejecución y si ha habido algún error.
   * @param spark SparkSession implícita del objeto principal App. Se usa para escribir en una tabla
   *              con spark.write.
   */
  def insertProcessValidationLog(db_schema: String,
                                 db_table_id_name: (String, String),
                                 validation_id: Int,
                                 type_validation: String,
                                 validation_msg: String,
                                 field_name: String,
                                 incidences: Int,
                                 flag: Double
                                )(implicit spark: SparkSession): Unit = {

    // Importar implicits para poder utilizar .toDF en Seq()
    import spark.implicits._

    // Crear la fila que se insertará en process_validation_logs
    val log_row: DataFrame = Seq(
      (getLatestIdTrigger(db_schema),
        validation_id,
        "yiyit_bigdata",
        type_validation,
        db_table_id_name._2,
        validation_msg,
        field_name,
        incidences,
        flag,
        Timestamp.valueOf(LocalDateTime.now()),
        Date.valueOf(LocalDate.now()),
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")).toLong)
    ).toDF(
      "id_trigger",
      "validation_id",
      "source",
      "type_validation",
      "file_name",
      "validation_msg",
      "field_name",
      "incidences",
      "flag",
      "execution_timestamp",
      "date_load",
      "timestamp_load"
    )

    // Insertar la fila
    insertRow(log_row, s"$db_schema.process_validation_logs")
  }

  /**
   * Devuelve el último ID registrado en trigger_control. Se utiliza para determinar el ID del siguiente log.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param sparkSession SparkSession implícita del objeto principal App. Se usa leer de la tabla
   *                     trigger_controlcon spark.write.
   * @return ID del último ID registrado en trigger_control
   */
  private def getLatestIdTrigger(db_schema: String)(implicit sparkSession: SparkSession): Int = {
    // Se obtiene el id_trigger con valor más alto que será utilizado para inserts en process_validation_logs
    val max_id_trigger: Int = readTable(s"$db_schema.trigger_control")
      .agg(max("id_trigger").alias("max_id_trigger"))
      .collect()(0).get(0).toString.toInt

    max_id_trigger // Devolver el ID con el valor más alto (el más reciente)
  }
}
