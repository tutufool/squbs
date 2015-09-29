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
package org.squbs.pipeline

import akka.actor._
import akka.pattern._
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}
import org.squbs.pipeline.Timeouts._
import spray.http._

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

class PipelineExecutorSpec extends TestKit(ActorSystem("PipelineExecutorSpec")) with FlatSpecLike
with Matchers with BeforeAndAfterAll with ImplicitSender with BeforeAndAfterEach {

  val agent = system.actorOf(Props(new AgentActor))

  val target: HttpRequest => Future[HttpResponse] = req => Future.successful(HttpResponse(StatusCodes.OK, "Hello"))
  val badTarget: HttpRequest => Future[HttpResponse] = req => Future.failed(new IllegalStateException("BadMan"))
  val messages = ListBuffer.empty[String]

  override protected def beforeEach() = messages.clear()


  "normal flow" should "work" in {
    agent ! "normal_flow"
    expectMsgPF(awaitMax) {
      case HttpResponse(StatusCodes.OK, entity, _, _) =>
        entity.data.asString(HttpCharsets.`UTF-8`) should be("Hello")
    }

    messages.size should be(6)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("preOutbound")
    messages(4) should be("outbound")
    messages(5) should be("postOutbound")

  }

  "preInbound shortcut" should "work" in {
    agent ! "preInbound_shortcut"
    expectMsgPF(awaitMax) {
      case HttpResponse(StatusCodes.Unauthorized, entity, _, _) =>
        entity.data.asString(HttpCharsets.`UTF-8`) should be("Reject")
    }

    messages.size should be(1)
    messages(0) should be("postOutbound")

  }

  "inbound shortcut" should "work" in {
    agent ! "inbound_shortcut"
    expectMsgPF(awaitMax) {
      case HttpResponse(StatusCodes.Unauthorized, entity, _, _) =>
        entity.data.asString(HttpCharsets.`UTF-8`) should be("Reject")
    }

    messages.size should be(2)
    messages(0) should be("preInbound")
    messages(1) should be("postOutbound")

  }

  "inbound shortcut with ExceptionalResponse" should "work" in {
    agent ! "inbound_shortcut_exceptional"
    expectMsgPF(awaitMax) {
      case HttpResponse(StatusCodes.BadRequest, entity, _, _) =>
        entity.data.asString(HttpCharsets.`UTF-8`) should be("inbound_shortcut_exceptional")
    }

    messages.size should be(2)
    messages(0) should be("preInbound")
    messages(1) should be("postOutbound")

  }

  "preInbound shortcut with chunk response" should "work" in {
    agent ! "preInbound_shortcut_chunk"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should startWith("Unsupported response: ChunkedMessageEnd")
    }

    messages.size should be(1)
    messages(0) should be("postOutbound")

  }

  "outbound unknown" should "work" in {
    agent ! "outbound_unknown"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should startWith("Unsupported response: org.squbs.pipeline.ResponseNotReady")
    }

    messages.size should be(5)

  }

  "preInbound fail" should "work" in {
    agent ! "preInbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("preInbound exception")
    }

    messages.size should be(1)
    messages(0) should be("postOutbound")

  }

  "inbound fail" should "work" in {
    agent ! "inbound_fail"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("inbound failed")
    }

    messages.size should be(2)
    messages(0) should be("preInbound")
    messages(1) should be("postOutbound")

  }

  "inbound exception" should "work" in {
    agent ! "inbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("inbound exception")
    }

    messages.size should be(2)
    messages(0) should be("preInbound")
    messages(1) should be("postOutbound")

  }

  "postInbound exception" should "work" in {
    agent ! "postInbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("postInbound exception")
    }

    messages.size should be(3)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postOutbound")

  }

  "target fail" should "work" in {
    agent ! "target_fail"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalStateException]
        t.getMessage should be("BadMan")
    }

    messages.size should be(5)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("preOutbound")
    messages(4) should be("postOutbound")
  }

  "preOutbound exception" should "work" in {
    agent ! "preOutbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("preOutbound exception")
    }

    messages.size should be(4)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("postOutbound")

  }

  "outbound fail" should "work" in {
    agent ! "outbound_fail"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("outbound failed")
    }

    messages.size should be(5)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("preOutbound")
    messages(4) should be("postOutbound")

  }

  "outbound exception" should "work" in {
    agent ! "outbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("outbound exception")
    }

    messages.size should be(5)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("preOutbound")
    messages(4) should be("postOutbound")

  }

  "postOutbound exception" should "work" in {
    agent ! "postOutbound_exception"
    expectMsgPF(awaitMax) {
      case Status.Failure(t) =>
        t.isInstanceOf[IllegalArgumentException]
        t.getMessage should be("postOutbound exception")
    }

    messages.size should be(5)
    messages(0) should be("preInbound")
    messages(1) should be("inbound")
    messages(2) should be("postInbound")
    messages(3) should be("preOutbound")
    messages(4) should be("outbound")

  }

  class AgentActor extends Actor {
    implicit val exec = context.dispatcher

    override def receive: Receive = {

      case "normal_flow" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "preInbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def preInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            throw new IllegalArgumentException("preInbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "inbound_fail" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def inbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            Future.failed(new IllegalArgumentException("inbound failed"))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "inbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def inbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            throw new IllegalArgumentException("inbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "postInbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def postInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            throw new IllegalArgumentException("postInbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "target_fail" =>
        val executor = new PipelineExecutor(badTarget, new AgentProcessor() {
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "preOutbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def preOutbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            throw new IllegalArgumentException("preOutbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()


      case "outbound_fail" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def outbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            Future.failed(new IllegalArgumentException("outbound failed"))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "outbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def outbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            throw new IllegalArgumentException("outbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "postOutbound_exception" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def postOutbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            throw new IllegalArgumentException("postOutbound exception")
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "preInbound_shortcut" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def preInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            ctx.copy(response = NormalResponse(HttpResponse(StatusCodes.Unauthorized, "Reject")))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "preInbound_shortcut_chunk" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def preInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
            ctx.copy(response = NormalResponse(ChunkedMessageEnd))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "inbound_shortcut" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def inbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            Future.successful(reqCtx.copy(response = NormalResponse(HttpResponse(StatusCodes.Unauthorized, "Reject"))))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "inbound_shortcut_exceptional" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def inbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            Future.successful(reqCtx.copy(response = ExceptionalResponse(HttpResponse(StatusCodes.BadRequest, "inbound_shortcut_exceptional"))))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

      case "outbound_unknown" =>
        val executor = new PipelineExecutor(target, new AgentProcessor() {
          override def outbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
            Future.successful(reqCtx.copy(response = ResponseNotReady))
          }
        })
        executor.execute(HttpRequest()) pipeTo sender()

    }
  }

  class AgentProcessor extends Processor {
    //inbound processing
    def inbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
      messages += "inbound"
      Future.successful(reqCtx)
    }

    //outbound processing
    def outbound(reqCtx: RequestContext)(implicit executor: ExecutionContext, context: ActorContext): Future[RequestContext] = {
      messages += "outbound"
      Future.successful(reqCtx)
    }

    //first chance to handle input request before processing request
    override def preInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
      messages += "preInbound"
      ctx
    }

    //last chance to handle input request before sending request to underlying service
    override def postInbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
      messages += "postInbound"
      ctx
    }

    //first chance to handle response before executing outbound
    override def preOutbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
      messages += "preOutbound"
      ctx
    }

    //last chance to handle output after executing outbound
    override def postOutbound(ctx: RequestContext)(implicit context: ActorContext): RequestContext = {
      messages += "postOutbound"
      ctx
    }


  }

}



