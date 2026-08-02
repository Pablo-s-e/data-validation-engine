package org.yiyit.time_tracker

/**
 * Contiene variables y funciones para monitorizar el tiempo de ejecución de las validaciones.
 */
class TimeTracker {

  private var first_timestamp: Double = 0
  private var table_start_timestamp: Double = 0
  private var latest_timestamp: Double = 0

  /**
   * Cambia el valor de $latest_timestamp y $first_timestamp al tiempo actual en milisegundos.
   * Se utiliza para tener la variable $first_timestamp como punto de partida.
   */
  def setInitialTimestamp(): Unit = {
    val ts = System.nanoTime()
    this.latest_timestamp = ts / 1000000000.0
    this.first_timestamp = ts / 1000000000.0
  }

  /**
   * Cambia el valor de $latest_timestamp y table_start_timestamp al tiempo actual en milisegundos.
   * Se utiliza para tener la variable $table_start_timestamp como punto de partida del inicio de
   * las validaciones de cada tabla.
   */
  def setTableStartTimestamp(): Unit = {
    val ts = System.nanoTime()
    this.latest_timestamp = ts / 1000000000.0
    this.table_start_timestamp = ts / 1000000000.0
  }

  /**
   * Mide el tiempo que ha pasado desde la última vez que se ha medido y guardado
   * en $latest_timestamp y sustituye el valor de $latest_timestamp por el actual.
   *
   * @return Número de milisegundos que han pasado en tipo Double.
   */
  def measureTimeElapsed(): Double = {
    val current_time = System.nanoTime() / 1000000000.0
    val time_elapsed = current_time - this.latest_timestamp
    this.latest_timestamp = current_time
    time_elapsed
  }

  /**
   * Mide el tiempo que ha pasado desde el inicio de las validaciones de una tabla, guardado en
   * $table_start_timestamp.
   *
   * @return Número de milisegundos que han pasado en tipo Double.
   */
  def measureTableTimeElapsed(): Double = {
    val current_time = System.nanoTime() / 1000000000.0
    val time_elapsed = current_time - this.table_start_timestamp
    time_elapsed
  }

  /**
   * Mide el tiempo que ha pasado desde la primera vez que se ha medido, guardado en $first_timestamp.
   *
   * @return Número de milisegundos que han pasado en tipo Double.
   */
  def measureFullTimeElapsed(): Double = {
    val current_time = System.nanoTime() / 1000000000.0
    val time_elapsed = current_time - this.first_timestamp
    time_elapsed
  }
}
