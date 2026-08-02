package org.yiyit.validations

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yiyit.logs.LogInserts.{insertProcessValidationLog, insertTriggerControlFlag}
import org.yiyit.time_tracker.TimeTracker

import scala.collection.mutable.ArrayBuffer

/**
 * Contiene la función que valida los requisitos funcionales de la tabla.
 */
object FunctionalValidation {

  /**
   * Valida los requisitos funcionales de la tabla.
   *
   * @param db_schema Schema de la base de datos de las tablas donde se insertan los logs.
   * @param db_table_id_name Tupla con ID y nombre de la tabla que se va a validar.
   * @param df Dataframe la tabla que se va a validar.
   * @param row_count Número de filas de la tabla.
   * @param spark SparkSession implícita del objeto principal App.
   * @return Boolean para comprobar si la validación se ha superado.
   */
  def validateFunctionalities(db_schema: String,
                              db_table_id_name: (String, String),
                              df: DataFrame,
                              row_count: Int
                             )(implicit spark: SparkSession): Boolean = {

    // Tomar el tiempo al iniciar la validación
    val time_tracker = new TimeTracker
    time_tracker.setInitialTimestamp()

    // Variables para monitorizar la ejecución
    var valid_table: Boolean = true
    val validation_id: Int = 4
    val type_validation: String = "Functional"
    var flag: Double = 1.4
    var error_msg: String = ""
    var incidences = 0
    val error_field_names = new ArrayBuffer[String]()

    /* Requisitos de las validaciones técnicas:
    1. Se debe validar que para cada par de valores distintos (template_code, Sheet) exista exactamente
       un registro en la columna data_name con el valor:
       - data_as_of
       - cristine_unit
       - excel_title
    2. Se debe validar que para cada par de valores distintos (template_code, Sheet) exista al menos un
       registro en la columna data_name que comience por “ccy”.
    3. Los valores de la columna column_x deben comenzar por “_c”.
    4. Se debe verificar que, dado el valor de la columna excel_cell, la posición indicada (por ejemplo,
       "B3") esté dentro del rango válido de la tabla, es decir, que el número de columna y fila derivados
       de la notación Excel no excedan las dimensiones reales de la tabla.
    */

    // Validación técnica 1
    // Se debe validar que para cada par de valores distintos (template_code, sheet)
    // exista exactamente un registro en la columna data_name con el valor:
    // - data_as_of
    // - cristine_unit
    // - excel_title

    println("Se validará que para cada par de valores (template_code, sheet) haya exactamente 1 registro " +
      "de data_name con el valor:\n'data_as_of', 'cristine_unit' y 'excel_title'.")

    // Guardar la lista de valores de data_name aceptables
    val data_name_values = Seq("data_as_of", "cristine_unit", "excel_title")

    // Coger las 3 columnas del dataframe, filtrar por registros con data_name = a los 3 valores que se buscan
    val filtered_data_name_df = df.select("template_code", "sheet", "data_name")
      .where(
        col("data_name").isin(data_name_values: _*)
      )

    // Comprobar si el dataframe filtrado con los 3 posibles valores de data_name está vacío
    val filtered_data_name_is_empty: Boolean = filtered_data_name_df.isEmpty

    println(s"Hay filas con valores de data_name en:" +
      s"\n'data_as_of', 'cristine_unit' y 'excel_title': $filtered_data_name_is_empty")

    // Si está vacío, no se supera la validación
    if (filtered_data_name_is_empty) {
      // Cambiar valid_table (false), error_msg e insertar las columnas que han dado error a error_field_names
      valid_table = false
      error_msg = "No hay ningún registro de data_name = 'data_as_of', 'cristine_unit' o 'excel_title'. "
      error_field_names.appendAll(Seq("template_code", "sheet", "data_name"))
      incidences += 1
      println("No hay ningún registro de data_name = 'data_as_of', 'cristine_unit' o 'excel_title'." +
        "\nNo se superarán las validaciones funcionales.")
    } else {
      // Si no está vacío, se comprueba que haya los valores suficientes y que sean correctos
      // Guardar en cache porque se usa varias veces
      println("Se guardará el dataframe filtrado en caché.")
      filtered_data_name_df.persist()
      println(f"Se ha guardado el dataframe filtrado en caché. (${time_tracker.measureTimeElapsed()}%.3f segundos)")

      // Usando el df filtrado:

      // Filtrar el df por aquellas filas que no son registros únicos
      val data_name_no_excess_rows: Boolean = filtered_data_name_df
        .groupBy("template_code", "sheet", "data_name")
        .count()
        .where(col("count") =!= 1)
        .limit(1)
        .isEmpty

      println(s"Al menos 1 de 'data_as_of', 'cristine_unit' y 'excel_title' no es único: $data_name_no_excess_rows")

      // Contar el nº de combinaciones (template_code, sheet) que no tienen el mismo nº de filas
      // que valores debe haber (3 en este caso) o que tienen 3 pero no son los esperados
      val data_name_no_missing_rows: Boolean = filtered_data_name_df
        .groupBy("template_code", "sheet")
        .agg(collect_set("data_name").alias("existing_data_names")) // Agregar en una lista por data_name
        .where(
          size(col("existing_data_names")) =!= data_name_values.size || // Si el tamaño de la lista^ no es el esperado
            !array_sort(col("existing_data_names"))
              .equalTo(array_sort(array(data_name_values.map(lit): _*))) // Si el array de valores no es el esperado
        )
        .limit(1)
        .isEmpty

      println(s"Falta al menos 1 de 'data_as_of', 'cristine_unit' y 'excel_title': $data_name_no_missing_rows")

      // Si count no es 1, significa que la combinación no tiene exactamente 1 registro y no se supera la validación
      if (!data_name_no_excess_rows || !data_name_no_missing_rows) {
        // Cambiar valid_table (false), error_msg e insertar las columnas que han dado error a error_field_names
        valid_table = false
        error_msg += "Cada par de valores (template_code, sheet) no tiene exactamente un registro " +
          "de data_name = 'data_as_of', otro = 'cristine_unit' y otro = 'excel_title'. "
        error_field_names.appendAll(Seq("template_code", "sheet", "data_name"))
        incidences += 1
        println("Cada par de valores (template_code, sheet) no tiene exactamente un registro " +
          "de data_name = 'data_as_of', otro = 'cristine_unit' y otro = 'excel_title'." +
          "\nNo se superarán las validaciones funcionales.")
      } else {
        println("Cada par de valores (template_code, sheet) sí tiene exactamente 1 registro de los 3 tipos.")
      }

      println(f"Se ha comprobado si para cada par de valores (template_code, sheet) hay exactamente 1 registro de " +
        f"data_name con el valor: 'data_as_of', 'cristine_unit' y 'excel_title'. (${time_tracker.measureTimeElapsed()}%.3f segundos)")

      println(f"Se borrará el dataframe filtrado del caché.")

      // Unpersist el dataframe filtrado ya que no se va a utilizar más
      filtered_data_name_df.unpersist()

      println(f"Se ha borrado el dataframe filtrado del caché. (${time_tracker.measureTimeElapsed()}%.3f segundos)")
    }


    // Validación técnica 2
    // Se debe validar que para cada par de valores distintos (template_code, Sheet)
    // exista al menos un registro en la columna data_name que comience por “ccy”.
    println("Se validará que para cada par de valores distintos (template_code, Sheet) " +
      "exista al menos un registro en la columna data_name que comience por “ccy”")

    // Contar el nº de pares únicos que hay
    val pairs_df_count = df.select("template_code", "sheet")
      .distinct()
      .count().toInt

    // Contar el nº de pares únicos que hay con al menos un registro de data_name ccy
    val ccy_pairs_df_count = df.where(col("data_name").like("ccy%"))
      .select("template_code", "sheet")
      .distinct()
      .count().toInt

    println(s"Nº de pares (template_code, sheet): $pairs_df_count" +
      s"\nNº de pares (template con al menos 1 registro 'ccy': $ccy_pairs_df_count")

    // Si el nº de pares es distinto, significa que hay algunos que no tienen al menos 1 registro data_name = 'ccy'
    if (pairs_df_count != ccy_pairs_df_count) {
      // Cambiar valid_table (false), error_msg e insertar las columnas que han dado error a error_field_names
      valid_table = false
      error_msg += "Cada par de valores (template_code, sheet) no tiene al menos " +
        "un registro de data_name que empiece por 'ccy'. "
      error_field_names.appendAll(Seq("template_code", "sheet", "data_name"))
      incidences += (pairs_df_count - ccy_pairs_df_count)
      println("Cada par de valores (template_code, sheet) no tiene al menos " +
        "un registro de data_name que empiece por 'ccy'. " +
        "\nNo se superarán las validaciones funcionales.")
    } else {
      println("Cada par de valores (template_code, sheet) tiene al menos " +
        "un registro de data_name que empiece por 'ccy'. ")
    }

    // Mostrar el tiempo que se ha tardado en la 2ª validación técnica
    println(f"Se ha validado que para cada par de valores distintos (template_code, Sheet) " +
      f"exista al menos un registro en la columna data_name que comience por “ccy”. " +
      f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Validación técnica 3
    // Los valores de la columna column_x deben comenzar por “_c”.
    println("Se validará que todos los valores de la columna column_x empiecen por “_c”.")

    // Filtrar el dataframe por filas en las que column_x no empiece por _c
    val column_x_no_odd_rows: Boolean = df.select("column_x")
      .where(!col("column_x").like("_c%"))
      .limit(1)
      .isEmpty

    // Si se cuenta al menos 1 registro de "column_x" que NO empieza por "_c" no se supera la validación
    if (!column_x_no_odd_rows) {
      // Cambiar valid_table (false), error_msg e insertar las columnas que han dado error a error_field_names
      valid_table = false
      error_msg += "No todos los valores de column_x empiezan por '_c'. "
      error_field_names.append("column_x")
      incidences += 1
      println("No todos los valores de column_x empiezan por '_c'. " +
        "\nNo se superarán las validaciones funcionales.")
    } else {
      println("Todas los valores de column_x empiezan por '_c'")
    }

    // Mostrar el tiempo que se ha tardado en la 3ª validación técnica
    println(f"Se ha validado que todos los valores de la columna column_x empiecen por “_c”. " +
      f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Validación técnica 4
    // Se debe verificar que, dado el valor de la columna excel_cell,
    // la posición indicada (por ejemplo, "B3") esté dentro del rango válido de la tabla,
    // es decir, que el número de columna y fila derivados de la notación Excel no excedan
    // las dimensiones reales de la tabla.
    println("Se validará que los valores indicados en excel_cell estén dentro de los límites.")

    // Guardar el nº de columnas y de filas del dataframe
    val column_count: Int = df.columns.length
    // "row_count" ya guarda el nº de filas del dataframe

    /*
     Primero seleccionar excel_cell del dataframe, después crear columnas separando letras y números de cada
     valor de excell_cell, luego crear una columna que cree el nº de columna a partir de las letras con ascii:
       *No he podido importar aggregate() como parte de spark.sql.functions, por eso está dentro de expr()
     Utilizo "aggregate()" para sumar los valores ascii de cada letra, si es que hay más de una,
     con los parámetros:
     - sequence(1, length(col_letters)): crea una lista del 1 hasta la longitud del string de letras, y así
        poder iterar sobre el número de letras que haya
     - 0 es el valor inicial del acumulador, que sumará 1 por cada elemento de la lista y se utilizará para
        determinar después el multiplicador del valor de la letra.
        (Por ejemplo AA -> la 1ª vale "1", la 2ª vale "27")
     - (acc, i) -> (ascii(substr(col_letters, i, 1)) - 64) + acc * 26:
        Primero se determina el valor de ascii de la letra actual. La letra actual se obtiene con el substring
        del valor de col_letters, cogiendo a partir del índice de la letra y de longitud 1, y después restando
        64 que es el valor de la letra A (solo se utilizan las letras mayúsculas).
        Luego, se multiplica por 26 el valor acumulado (al llegar a la siguiente letra significa que ya se han
        pasado por los valores anteriores, por ejemplo, "BA" significa que se a pasado de A-Z y de AA-AZ, que
        serían otras 26 combinaciones) y se sumaría al valor de esta letra. Ya que el acumulador empieza en 0,
        y se multiplica por el acumulador, si el string de las letras es solo de longitud 1, no se sumaría
      Por último, se filtra por aquellos registros cuyos valores de row_num o column_count sean superiores
      al máximo de la tabla (definido en las variables row_count y column_count).
      Si hay alguna fila después de filtrar, significa que al menos 1 valor estaría fuera del límite de la tabla
    */
    val no_oob_excel_cells: Boolean = df.select("excel_cell")
      .withColumn("col_letters", upper(regexp_extract(col("excel_cell"), "([A-Za-z]+)([0-9]+)", 1)))
      .withColumn("row_num", regexp_extract(col("excel_cell"), "([A-Za-z]+)([0-9]+)", 2))
      .withColumn("col_num",
        expr("aggregate(" +
          "sequence(1, length(col_letters)), " +
          "0, " +
          "(acc, i) -> (ascii(substr(col_letters, i, 1)) - 64) + acc * 26 )"
        )
      )
      .where(col("row_num") >= row_count || col("col_num") >= column_count)
      .limit(1)
      .isEmpty

    // Mostrar el resultado de la 4ª validación técnica
    println(s"Hay registros con columnas o filas out of bounds: ${!no_oob_excel_cells}")

    // Si hay al menos una celda fuera del rango, la tabla no es válida (false)
    if (!no_oob_excel_cells) {
      // Cambiar valid_table (false), error_msg e insertar las columnas que han dado error a error_field_names
      valid_table = false
      error_msg += "Hay valores en 'excel_cell' fuera del rango de la tabla. "
      error_field_names.append("excel_cell")
      incidences += 1
      println("Hay valores en 'excel_cell' fuera del rango de la tabla. " +
        "\nNo se superarán las validaciones funcionales.")
    } else {
      println("Todos los valores de 'excel_cell' están dentro del rango de la tabla.")
    }

    // Mostrar el tiempo que se ha tardado en la 4ª validación técnica
    println(f"Se ha validado que los valores indicados en excel_cell estén dentro de los límites. " +
      f"(${time_tracker.measureTimeElapsed()}%.3f segundos)")

    // Comprobar si se han superado la validaciones técnicas
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
      flag = 34
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
        field_name = if (error_field_names.nonEmpty) {error_field_names.distinct.mkString(", ")} else null,
        incidences = incidences,
        flag = flag)
    }

    valid_table // Devolver si la tabla ha superado o no la validación
  }
}
