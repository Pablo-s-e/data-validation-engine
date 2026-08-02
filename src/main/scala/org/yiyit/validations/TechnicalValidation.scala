package org.yiyit.validations

import org.apache.spark.sql.functions.{col, length, lit, to_date}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.yiyit.logs.LogInserts.{insertProcessValidationLog, insertTriggerControlFlag}
import org.yiyit.time_tracker.TimeTracker

import scala.collection.mutable.ArrayBuffer

/**
 * Contiene la función de validación de tipos de dato y de primary keys.
 */
object TechnicalValidation {

  /**
   * Valida los tipos de dato de las columnas del dataframe de la tabla y comprueba que las primary
   * keys sean únicas.
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
  def dataTypeValidation(db_schema: String,
                         db_table_id_name: (String, String),
                         df: DataFrame,
                         sl_df: DataFrame,
                         row_count: Int)(implicit spark: SparkSession): Boolean = {

    // Tomar el tiempo al iniciar la validación
    val time_tracker = new TimeTracker
    time_tracker.setInitialTimestamp()

    // Variables para monitorizar la ejecución
    var valid_table: Boolean = true
    val validation_id: Int = 2
    val type_validation: String = "Technical"
    var flag: Double = 1.2
    var error_msg: String = ""
    var incidences = 0
    val error_field_names = new ArrayBuffer[String]()

    // Validación de tipos de dato
    // Obtener los nombres de las columnas en semantic_layer
    val sl_cols_df = sl_df
      .select("id_type_file", "field_position", "field_name", "data_type",
              "decimal_symbol", "nullable", "length", "pk")
      .where(col("id_type_file") === db_table_id_name._1)
      .orderBy("field_position")
      .toDF()

    // Guardar el número de columnas e inicializar el contador para luego mostrarlo por consola
    var contador_columnas: Int = 0

    // Obtener la información de las columnas desde semantic_layer y mostrarla
    println(s"Información de las columnas de la tabla:")

    // Recorrer las filas de semantic_layer con la info de las columnas y devolver una expresión para cada una
    val combined_col_expr: Column = sl_cols_df.collect().map { sl_row =>

      // Coger los valores de semantic_layer
      // Obtener el nombre de la columna
      val col_name = sl_row.getAs[String]("field_name")
      // Obtener el tipo de dato
      val data_type = sl_row.getAs[String]("data_type")
      // Obtener si es nullable o no
      val is_nullable = sl_row.getAs[Boolean]("nullable")
      // Obtener la longitud máxima
      val field_length = Option(sl_row.getAs[String]("length").toInt).getOrElse(0)

      // Añadir al contador de columnas
      contador_columnas += 1

      // Mostrar los datos de la columna
      println(
        s"Columna $contador_columnas: $col_name | " +
        s"Tipo de dato: $data_type | " +
        s"Nullable: $is_nullable | " +
        s"Length: $field_length")

      // Comprobación de tipo de dato
      val col_expr: Column = data_type match {
        // Tipo de dato String
        case "STRING" =>
          // Comprobar si es nullable
          if (is_nullable) {
            // Si es nullable, comprobar si excede la longitud máxima
            length(col(col_name)) > field_length
          } else {
            // Si no es nullable, comprobar si es nulo o si excede la longitud máxima
            col(col_name).isNull || length(col(col_name)) > field_length
          }

        // Tipo de dato INT
        case "INT" =>
          // Comprobar si es nullable
          if (is_nullable) {
            // Si es nullable, comprobar si excede la longitud máxima o si falla el cast a int
            length(col(col_name)) > field_length || !col(col_name).rlike(s"^-?\\d{1,$field_length}$$")
          } else {
            // Si no es nullable, comprobar si es nulo o si excede la longitud máxima o si falla el cast a int
            col(col_name).isNull || length(col(col_name)) > field_length || !col(col_name).rlike(s"^-?\\d{1,$field_length}$$") // col(col_name).cast("int").isNull
          }

        // Tipo de dato Decimal
        case decimal if decimal.startsWith("DECIMAL") =>
          // Primero comprobar si la declaración de tipo de dato es correcta y obtener los dígitos
          // Definir el patrón regex y que coja los 2 dígitos
          val decimal_pattern = """DECIMAL\((\d+),(\d+)\)""".r
          // Comparar el valor de data_type con el decimal_pattern
          decimal match {
            // Comprobar si el valor de decimal coincide con el patrón regex
            case decimal_pattern(x, y) =>
              // Si el valor coincide con el patrón regex, obtener el decimal_symbol y crear la expr
              // regex para el campo
              // Obtener el separador
              val decimal_symbol = Option(sl_row.getAs[String]("decimal_symbol")).getOrElse("\\.")
              println(s"El decimal_symbol es $decimal_symbol")

              // Definir el patrón que tienen que tener los valores del campo
              val decimal_regex = s"^-?\\d{1,${x.toInt - y.toInt}}$decimal_symbol\\d{${y.toInt}}$$"

              // Comprobar si es nullable
              if (is_nullable) {
                // Si es nullable, comprobar si excede la longitud máxima o si no coincide con la expresión regex
                length(col(col_name)) > field_length || !col(col_name).rlike(decimal_regex)
              } else {
                // Si no es nullable, comprobar si es nulo o si excede la longitud máxima
                // o si no coincide con la expresión regex
                col(col_name).isNull || length(col(col_name)) > field_length || !col(col_name).rlike(decimal_regex)
              }

            // Si el tipo de dato no coincide con el patrón regex, la columna no es válida y hay error (true)
            case _ => lit(true)
          }

        // Tipo de dato Date
        case date if date.startsWith("DATE") =>
          val date_format_regex = "^\\d{4}-(0[0-9]|1[0-2])-\\d{2}\\s[+-](0[0-9]|1[0-4])$"
          // Comprobar si es nullable
          if (is_nullable) {
            // Si es nullable, comprobar si excede la longitud máxima o si falla el cast a date
            length(col(col_name)) > field_length || col(col_name).cast("date").isNull ||
              !col(col_name).rlike(date_format_regex)
          } else {
            // Si no es nullable, comprobar si es nulo o si excede la longitud máxima o si falla el cast a date
            col(col_name).isNull || length(col(col_name)) > field_length || col(col_name).cast("date").isNull ||
              !col(col_name).rlike(date_format_regex)
          }

        // Si el tipo de dato no se reconoce, la columna no es válida y hay error (true)
        case _ => lit(true)
      }

      col_expr // Devolver las condiciones
    }.reduce(_ || _) // Reducir el array a una única expresión con 'OR'

    //  Mostrar cuánto se ha tardado en obtener los filtros necesarios para validar los tipos de dato
    println(f"Se han obtenido los datos de semantic layer y generado expresiones para filtrar " +
      f"cada columna. (${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Mostrar las expresiones obtenidas para filtrar
    println("Condiciones: " + combined_col_expr)

    // Filtrar el dataframe según las expresiones de cada columna generando un array.
    // Quitar los nulos y si el array tiene elementos es que ha habido algún error de tipo de dato
    // Obtener la primera fila y guardar si está vacía en un boolean
    val has_datatype_error: Boolean =
      !df.withColumn("val_errors", combined_col_expr)
        .where(col("val_errors"))
        .limit(1)
        .isEmpty

    // Mostrar cuánto se ha tardado en filtrar el dataframe para comprobar si hay errores de tipo de dato
    println(f"Se ha filtrado el dataframe y comprobado si hay alguna fila errónea: $has_datatype_error. " +
      f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Comprobar el número de errores
    if (has_datatype_error) {
      // Si hay al menos un error, cambiar valid_table a false
      valid_table = false
      error_msg = "Los tipos de dato no son correctos"
      println(s"La tabla ${db_table_id_name._2} no superará la segunda validación por error de tipo de dato")
    } else {
      println(s"Los tipos de dato de la tabla ${db_table_id_name._2} son correctos")
    }

    // Validación de primary keys
    println("Validando primary keys...")

    // Guardar una lista de los campos que son pk
    import spark.implicits._
    val pk_fields = sl_cols_df.select("field_name", "pk")
      .where(col("pk") === true)
      .select("field_name")
      .as[String]
      .collect()

    // Comprobar si la tabla tiene primary keys
    if (pk_fields.isEmpty) {
      // Si NO tiene primary keys:
      println(s"La tabla ${db_table_id_name._2} no tiene primary keys.")
    } else {
      // Si SÍ tiene primary keys:
      // Hacer un count con distinct para ver si de verdad son unicas las filas segun las PKs
      val df_pk_unique_row_count = df.select(pk_fields.map(col): _*).distinct().count()

      println(s"Columnas que son primary key: ${pk_fields.mkString(", ")}")
      println(s"Count total de filas: $row_count | Count de filas con PKs únicas: $df_pk_unique_row_count")

      // Si el nº de filas es distinto, se cambia valid_table a false
      if (row_count != df_pk_unique_row_count) {
        valid_table = false
        error_msg += "Las PKs no son únicas"
        incidences += 1
        println(s"La tabla ${db_table_id_name._2} no superará la segunda validación por error de primary keys")
      } else {
        println(s"Las primary keys de la tabla ${db_table_id_name._2} son correctas")
      }
    }

    // Mostrar si las primary keys son válidas
    println(f"Se ha comprobado si las primary keys son válidas. " +
      f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Insertar en los logs en función de al conclusión de si la tabla es válida o no:
    if (valid_table) {
      // Si es válida, insertar en los logs (trigger_control)
      insertTriggerControlFlag(
        db_schema = db_schema,
        db_table_id_name = db_table_id_name,
        flag = flag,
        row_count = row_count)
    } else {
      // Si no es válida:
      // - Cambiar el valor de flag
      flag = 32
      // Insertar en los logs (trigger_control y process_validation_logs)
      insertTriggerControlFlag(
        db_schema = db_schema,
        db_table_id_name = db_table_id_name,
        flag = flag,
        row_count = row_count)
      insertProcessValidationLog(
        db_schema = db_schema,
        db_table_id_name = db_table_id_name,
        validation_id = validation_id,
        type_validation = type_validation,
        validation_msg = error_msg,
        field_name = if (error_field_names.nonEmpty) {error_field_names.mkString(", ")} else null,
        incidences = incidences,
        flag = flag)
    }

    valid_table // Devolver si la tabla ha superado o no la validación
  }
}
