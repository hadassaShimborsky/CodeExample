package com.project.api.dao

import java.sql.DriverManager

import scala.Left
import scala.Right
import scala.collection.mutable.ArrayBuffer

import com.project.api.config.Configuration
import com.project.api.domain.Failure
import com.project.api.domain.FailureTreatment.exceptionError

import spray.json.JsArray
import spray.json.JsObject
import spray.json.JsString


class PostgresExample extends Configuration {

  
  /* Postgres client init */
  Class.forName("org.postgresql.Driver").newInstance

  def connectToPostgresql() = {
    DriverManager.getConnection(s"jdbc:postgresql://$postgresHost/$postgresLocationsTable", postgresUser, postgresPass)
  }

  
  /**
   * Get top x locations of supplied type nearest supplied location.
   * 
   * (
   * For example: get top 10 neighborhoods nearest the location we supplied. 
   * the call to the function will be:
   * 	getNearestLocations(<location latitude>, <location longitude>, "IL", "Neighborhood",10)
   * the sql query in the function will be:
   * 	"select Neighborhood from ilNeighborhood ORDER BY geom <-> st_setsrid(st_makepoint(<Double>,<Double>),4326) LIMIT 10;"
   * ) 
   *
   * @param lat: Double, lon: Double, country: String, locationType: String, limit: Int
   * @return on success: Right object of JsObject contains the query results.
   * 		 on failure: Left object of Failure contains the error message.
   */
  def getNearestLocations(lat: Double, lon: Double, country: String, locationType: String, limit: Int): Either[Failure, JsObject] = {
    try {
      /* Connect to postgres */
      val db = connectToPostgresql()
      val statement = db.createStatement      
      /* Concat the table name depends on the locationType */
      val table = s"${country.toLowerCase()}${locationType}"
      val resultArr = ArrayBuffer.empty[JsString]
      /* sql query */
      val sql = s"""select $locationType
			      FROM $table 
			      ORDER BY geom <-> st_setsrid(st_makepoint($lon,$lat),4326) 
			      LIMIT $limit;"""
      val resultSet = statement.executeQuery(sql)
      /* read the result into resultArr */
      while (resultSet.next()) {
        resultArr += JsString(resultSet.getString("neighborhood"))
      }
      Right(JsObject("Neighborhoods" -> JsArray(resultArr.toVector)))
    } catch {
      case e: Exception =>
        Left(exceptionError(e))
    }
  }


}