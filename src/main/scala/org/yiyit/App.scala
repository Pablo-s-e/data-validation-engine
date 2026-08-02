package org.yiyit

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.yiyit.connection.DbConnection.readTable
import org.yiyit.time_tracker.TimeTracker
import org.yiyit.logs.LogInserts.insertTriggerControlFlag
import org.yiyit.validations.TableValidation.validateTable
import org.yiyit.validations.TechnicalValidation.dataTypeValidation
import org.yiyit.validations.ReferentialIntegrityValidation.validateReferentialIntegrity
import org.yiyit.validations.FunctionalValidation.validateFunctionalities

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}


/**
 * @author ${pablo.serrano}
 */
object App {

  /**
   * Función principal que lleva el flujo de ejecución del programa.
   *
   * @param args String arguments.
   */
  def main(args : Array[String]): Unit = {

    // Instanciar la clase timetracker y comenzar tiempo de ejecución
    val main_tt = new TimeTracker
    main_tt.setInitialTimestamp()

    // Crear el objeto SparkSession
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("yiyit-big-data")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    // Definir el schema de la base de datos de las tabla por defecto
    val db_schema: String = "myschema_pabloserrano"

    // Leer la tabla file_configuration
    println("Leyendo y persistiendo la tabla 'file_configuration'...")

    val fc_df = readTable(s"$db_schema.file_configuration")
    fc_df.persist()

    println("Se ha leído y persistido la tabla 'file_configuration'.")

    // Leer la tabla semantic_layer
    println("Leyendo y persistiendo la tabla 'semantic_layer'...")

    val sl_df = readTable(s"$db_schema.semantic_layer")
    sl_df.persist()

    println("Se ha leído y persistido la tabla 'semantic_layer'.")

    // Importar implicits y obtener los nombres de las tablas
    println("Obteniendo los IDs y nombres de las tablas en 'file_configuration'...")

    import spark.implicits._
    val table_id_name_arr: Array[(String, String)] = fc_df
      .select("id_type_file", "type_file_name")
      .where(col("id_type_file") === 18)
      .orderBy("id_type_file")
      .as[(String, String)]
      .collect()

    // Mostrar los nombres de las tablas obtenidas
    println("Se han obtenido los IDs y los nombres de las tablas en 'file_configuration'." +
      "\nSe validarán las tablas:" +
      s"\n- ${table_id_name_arr.mkString("\n- ")}")

    // Crear un arraybuffer para guardar hasta qué validación ha llegado cada tabla
    // que guarda (nombre_tabla, nº validaciones recorridas)
    val table_progress = new ArrayBuffer[(String, Int)]()

    // Comienzo de las validaciones de cada tabla
    println(f"Iniciando las validaciones. Tiempo desde el inicio del programa: " +
      f"${main_tt.measureTimeElapsed()}%.3f segundos")

    // Bucle for que ejecuta las funciones de las validaciones para cada tabla
    table_id_name_arr.foreach { db_table_id_name =>
      // Crear un breakable para poder saltar el resto de validaciones si una tabla falla una validación
      breakable {
        // Variable que guarda si la tabla es válida
        var valid_table = true

        // Contador de validaciones recorridas
        var validation_count = 0

        // Tomar el tiempo al iniciar las validaciones de la tabla
        main_tt.setTableStartTimestamp()

        println(s"[V${db_table_id_name._1}] Se validará la tabla ${db_table_id_name._2}.")

        // Primera validación - validación de tabla
        println(s"[V${db_table_id_name._1}] Entrando a la validación de tabla...")

        // Devuelve un dataframe que se utiliza en el resto de validaciones si se supera,
        // un boolean para comprobar si se ha superado la validación, y el row_count de la tabla
        val table_validation_result: (DataFrame, Boolean, Int) = validateTable(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          fc_df = fc_df,
          sl_df = sl_df
        )

        // Mostrar lo que se ha tardado en validar el header
        println(f"[V${db_table_id_name._1}] Se ha validado el header de la tabla ${db_table_id_name._2}. " +
          f"(${main_tt.measureTimeElapsed()}%.3f segundos)")

        // Guardar los resultados de la validación en variables
        val clean_df: DataFrame = table_validation_result._1
        valid_table = table_validation_result._2
        val df_row_count = table_validation_result._3

        // Comprobar el resultado de la primera validación (true/false)
        if (valid_table) {
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} ha superado la " +
            s"primera validación (validación de tabla).")
        } else {
          // Si el valor de valid_table ha cambiado a false:
          // - Se añade el progreso al array_buffer
          table_progress.append((db_table_id_name._2, validation_count))
          // - Se salta a la siguiente iteración del bucle
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} no ha superado la " +
            s"primera validación (validación de tabla).")

          // Unpersist el dataframe y romper el bucle
          break()
        }

        // Aumentar el contador de validaciones superadas
        validation_count += 1

        // Persistir el dataframe de la tabla
        println(s"[V${db_table_id_name._1}] Persistiendo dataframe de la tabla ${db_table_id_name._2}...")

        clean_df.persist()
        clean_df.count()

        // Mostrar lo que se ha tardado en persistir el dataframe de la tabla
        println(f"[V${db_table_id_name._1}] Se ha persistido el dataframe de ${db_table_id_name._2}. " +
          f"(${main_tt.measureTimeElapsed()}%.3f segundos)")


        // Segunda validación - validación de tipos de dato
        println(s"[V${db_table_id_name._1}] Entrado a la validacion de tipos de dato...")

        // Devuelve un boolean para comprobar si se ha superado la validación
        valid_table = dataTypeValidation(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          df = clean_df,
          sl_df = sl_df,
          row_count = df_row_count
        )

        // Mostrar lo que se ha tardado en validar los tipos de dato y las primary keys
        println(f"[V${db_table_id_name._1}] Se han validado los tipos de dato y las primary keys " +
          f"de la tabla ${db_table_id_name._2}. (${main_tt.measureTimeElapsed()}%.3f segundos)")

        // Comprobar el resultado de la segunda validación (true/false)
        if (valid_table) {
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} ha superado la " +
            s"segunda validación (validación técnica).")
        } else {
          // Si el valor de valid_table ha cambiado a false:
          // - Se añade el progreso al array_buffer
          table_progress.append((db_table_id_name._2, validation_count))
          // - Se salta a la siguiente iteración del bucle
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} no ha superado la " +
            s"segunda validación (validación técnica).")

          // Unpersist el dataframe y romper el bucle
          clean_df.unpersist()
          break()
        }

        // Aumentar el contador de validaciones superadas
        validation_count += 1

        // Tercera validación - validación de integridad referencial
        println(s"[V${db_table_id_name._1}] Entrando a la validación de integridad referencial...")

        // Devuelve un boolean para comprobar si se ha superado la validación
        valid_table = validateReferentialIntegrity(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          df = clean_df,
          sl_df = sl_df,
          row_count = df_row_count
        )

        // Mostrar lo que se ha tardado en validar la integridad referencial
        println(f"[V${db_table_id_name._1}] Se ha validado la integridad referencial de las columnas " +
          f"de la tabla ${db_table_id_name._2}. (${main_tt.measureTimeElapsed()}%.3f segundos)")

        // Comprobar el resultado de la tercera validación (true/false)
        if (valid_table) {
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} ha superado la " +
            s"tercera validación (validación de integridad referencial).")
        } else {
          // Si el valor de valid_table ha cambiado a false:
          // - Se añade el progreso al array_buffer
          table_progress.append((db_table_id_name._2, validation_count))
          // - Se salta a la siguiente iteración del bucle
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} no ha superado la " +
            s"tercera validación (validación de integridad referencial).")

          // Unpersist el dataframe y romper el bucle
          clean_df.unpersist()
          break()
        }

        // Aumentar el contador de validaciones superadas
        validation_count += 1

        // Cuarta validación - validación funcional
        println(s"[V${db_table_id_name._1}] Entrando a las validaciones funcionales")

        // Devuelve un boolean para comprobar si se ha superado la validación
        valid_table = validateFunctionalities(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          df = clean_df,
          row_count = df_row_count
        )

        // Mostrar lo que se ha tardado en las validaciones funcionales
        println(f"[V${db_table_id_name._1}] Se han terminado las validaciones funcionales de la tabla " +
          f"${db_table_id_name._2}. (${main_tt.measureTimeElapsed()}%.3f segundos)")

        // Comprobar el resultado de la cuarta validación (true/false)
        if (valid_table) {
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} ha superado la " +
            s"cuarta validación (validaciones funcionales).")
        } else {
          // Si el valor de valid_table ha cambiado a false:
          // - Se añade el progreso al array_buffer
          table_progress.append((db_table_id_name._2, validation_count))
          // - Se salta a la siguiente iteración del bucle
          println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} no ha superado la " +
            s"cuarta validación (validaciones funcionales).")

          // Unpersist el dataframe y romper el bucle
          clean_df.unpersist()
          break()
        }

        // Aumentar el contador de validaciones superadas
        validation_count += 1

        // Unpersist el dataframe de la tabla al terminar de validar
        clean_df.unpersist()

        // Si se han superado todas las validaciones:
        println(s"[V${db_table_id_name._1}] La tabla ${db_table_id_name._2} ha superado todas las validaciones.")
        table_progress.append((db_table_id_name._2, validation_count))
        // Se inserta en la tabla trigger_control con flag = 2
        insertTriggerControlFlag(
          db_schema = db_schema,
          db_table_id_name = db_table_id_name,
          flag = 2,
          row_count = df_row_count
        )
      }

      // Mostrar el tiempo de ejecución de las validaciones de la tabla
      println(f"[V${db_table_id_name._1}] Tiempo desde el inicio de la validación de la tabla " +
        f"${db_table_id_name._2}: ${main_tt.measureTableTimeElapsed()}%.3f segundos.")
      println(s"[V${db_table_id_name._1}] Pasando a validar la siguiente tabla...")
    }

    println("Se han validado todas las tablas.")

    // Mostrar el progreso final de cada tabla
    println("Mostrando resultado final de las validaciones de todas las tablas:")
    table_progress.foreach { table_valprogress =>
      println(s"La tabla ${table_valprogress._1} ha superado ${table_valprogress._2} validaciones.")
    }

    // Mostrar el tiempo total de ejecución
    println(f"Fin de la ejecución. Programa ejecutado con éxito. " +
      f"(${main_tt.measureFullTimeElapsed()}%.3f segundos)")

  }
}
