/*
 *  Copyright 2015 PayPal
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.squbs.httpclient

import org.squbs.httpclient.env.{Default, Environment}
import org.squbs.httpclient.json.Json4sJacksonNoTypeHintsProtocol
import org.squbs.pipeline.PipelineSetting
import spray.http.{HttpRequest, Uri}
import spray.httpx.RequestBuilding
import spray.httpx.marshalling.Marshaller

object HttpClientManagerMessage {

  /**
   * Success => HttpClientActor
   * @param name
   * @param env
   */
  case class Get(name: String, env: Environment = Default)

  /**
   * Success => DeleteSuccess
   * Failure => HttpClientNotExistException
   * @param name
   * @param env
   */
  case class Delete(name: String, env: Environment = Default)

  case object DeleteSuccess

  /**
   * Success => DeleteAllSuccess
   */
  case object DeleteAll

  case object DeleteAllSuccess

  /**
   * Success => TrieMap[(String, Environment), HttpClient]
   */
  case object GetAll
}

object HttpClientActorMessage {

  /**
   * Success => HttpClientActor
   * @param config
   */
  case class UpdateConfig(config: Configuration)

  /**
   * Success => HttpClientActor
   * @param settings
   */
  case class UpdateSettings(settings: Settings)

  /**
   * Success => HttpClientActor
   * @param pipeline
   */
  case class UpdatePipeline(pipeline: Option[PipelineSetting])

  /**
   * Success => MarkDownSuccess
   */
  case object MarkDown

  case object MarkDownSuccess

  /**
   * Success => MarkUpSuccess
   */
  case object MarkUp

  case object MarkUpSuccess


  /**
   * Success => CloseSuccess
   */
  case object Close

  case object CloseSuccess

  sealed trait HttpClientMessage {
    def requestBuilder : Uri => HttpRequest
    def requestSettings : Option[RequestSettings]
    def uri : String
  }

  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   */
  case class Get(uri: String, requestSettings: Option[RequestSettings] = None) extends HttpClientMessage {
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Get(_)
  }

  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   */
  case class Options(uri: String, requestSettings: Option[RequestSettings] = None) extends HttpClientMessage {
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Options(_)
  }

  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   */
  case class Head(uri: String, requestSettings: Option[RequestSettings] = None) extends HttpClientMessage {
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Head(_)
  }

  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   */
  case class Delete(uri: String, requestSettings: Option[RequestSettings] = None) extends HttpClientMessage{
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Delete(_)
  }


  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   * @param content
   * @param marshaller
   * @tparam T
   */
  case class Post[T](uri: String, content: Option[T],
                     marshaller: Marshaller[T] = Json4sJacksonNoTypeHintsProtocol.json4sMarshaller,
                     requestSettings: Option[RequestSettings] = None) extends HttpClientMessage{
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Post(_, content)(marshaller)
  }

  /**
   * Success => HttpResponse
   * Failure => Throwable
   * @param uri
   * @param content
   * @param marshaller
   * @tparam T
   */
  case class Put[T](uri: String, content: Option[T],
                    marshaller: Marshaller[T] = Json4sJacksonNoTypeHintsProtocol.json4sMarshaller,
                    requestSettings: Option[RequestSettings] = None) extends HttpClientMessage {
    val requestBuilder : (Uri) => HttpRequest = RequestBuilding.Put(_, content)(marshaller)
  }
}