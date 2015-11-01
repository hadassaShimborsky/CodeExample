package com.trovimap.api.dao

import scala.Left
import scala.Right

import org.elasticsearch.common.settings.ImmutableSettings

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl.RichFuture
import com.sksamuel.elastic4s.ElasticDsl.SearchDefinitionExecutable
import com.sksamuel.elastic4s.ElasticDsl.search
import com.sksamuel.elastic4s.ElasticDsl.termQuery
import com.project.api.config.Configuration
import com.project.api.domain.Failure
import com.project.api.domain.FailureTreatment.exceptionError

import spray.json.JsObject
import spray.json.pimpString


class ESExample extends Configuration {
  
  
  /* ElastivSearch client init */
  protected val settings = ImmutableSettings.settingsBuilder().put("cluster.name", elasticSearchCluster).build()
  protected val ESclient = ElasticClient.remote(settings, (elasticSearchHost, elasticSearchPort))

  
  /**
   * Read property from ElasticSearch.
   *
   * @param propertyId:String
   * @return on success: Right object of JsObject contains the property.
   * 		 on failure: Left object of Failure contains the error message.
   */
  def readProperty(propertyId: String): Either[Failure, JsObject] = {
    try {
      /* Select property from ElasticSearch by Id. */
      val esResult = ESclient.execute {
        search in esPropertiesIndex query {
          termQuery("_id", propertyId)
        } size 1
      }.await
      /* Extract the property from the esult. */
      val resultProperty = esResult.getHits.getHits.map(f => f.sourceAsString.parseJson.asJsObject)
      /* If the result is not empty - return the property. else - manage error */
      resultProperty.length match {
        case 1 => Right(resultProperty(0))
        case _ => Left(exceptionError(new Exception(s"The property with id: $propertyId does not exist")))
      }
    } catch {
      case e: Exception =>
        Left(exceptionError(e))
    }
  }
  

}