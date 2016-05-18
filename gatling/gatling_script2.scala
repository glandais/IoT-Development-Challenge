import scala.concurrent.duration._
import scala.collection.breakOut

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._

import io.gatling.core.structure.PopulationBuilder

import java.text.SimpleDateFormat
import java.util.{ Locale, TimeZone }
import java.util.Calendar
import java.text.SimpleDateFormat

import java.time._
import java.time.format._
import java.util.UUID

import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost

import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder;

import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import scala.reflect.runtime.universe._
import java.math.BigInteger

/**
 * This class does the test by spraying a msgPackage x numOfPackages x 10 of http post requests to your service (/messages) Concurrently
 * verfies that your synthesis service works given a timestamp and a duration
 * verifies that your synthesis for the total of values sent  is correct,
 * calculates how much time the simulation took
 * sends the results to the leaderBoard (validate if you have the password)
 */
class InjectionsAndVerificationsGlandais extends Simulation {

  val sensors = 10;

  //number of packages sent by a injector
  val numOfPackages = 100

  //the number of messages in a  package
  val msgPackage = 100

  val hostIp = "127.0.0.1"
  //enter the name of your team
  val teamName = ""
  //enter the members of the team
  val teamMembers = ""
  //enter your location (Nantes, Paris, Brest etc...)
  val teameLocation = ""

  //HTTP Protocol

  val httpProtocol = http.baseURL("http://").inferHtmlResources()

  //this is the adress of the http post the adress is your rasberry pi adress
  var url = "http://" + hostIp + "/messages"

  //the Date formatter who makes the date on the DateTime RFC3339
  val formatter = DateTimeFormatter.ISO_INSTANT;

  //the simulation start in nano seconds
  var simulationStartTime: Long = 0
  //the simulation end in nano seconds
  var simulationEndTime: Long = 0
  var simulationStartTimeMs: Long = 0

  //setting the json parser to return big decimals
  scala.util.parsing.json.JSON.globalNumberParser = { input: String => BigDecimal(input) }

  /**
   * This Bloc of code runs before the simulation
   * it retreives the start time of the simulation
   */
  before {
    println("la simulation est sur le point de commencer...")
    simulationStartTimeMs = Calendar.getInstance().getTimeInMillis()
    simulationStartTime = System.nanoTime()
  }

  /**
   * this object is the scenario builder
   * a scenarios sends a msgPackage for all type sensors ...
   */
  object ScenarioBuilder {

    /**
     * this method generates a unique Id  for each message sent
     * @param Unit
     * @return a Unique UUID
     */
    def generateId(): String = {
      UUID.randomUUID().toString().replaceAll("-", "") + UUID.randomUUID().toString().replaceAll("-", "")
    }

    /**
     * generates a random value and saves the minimum/maximum/sum globaly
     * @param the Index of the sensorType
     * @return random value
     */
    def generateNum(): Long = {
      val r = scala.util.Random
      var value = r.nextInt(20000)
      return value
    }

    def min(session: Session, key: String, value: Long): Session = {
      session.set(key, Math.min(session(key).asOption[Long].getOrElse(Long.MaxValue), value))
    }

    def max(session: Session, key: String, value: Long): Session = {
      session.set(key, Math.max(session(key).asOption[Long].getOrElse(Long.MinValue), value))
    }

    def sum(session: Session, keyCount: String, keySum: String, value: Long): Session = {
      val count: Int = session(keyCount).asOption[Int].getOrElse(0)
      val sum: BigInteger = session(keySum).asOption[BigInteger].getOrElse(new BigInteger("0"))
      session.set(keyCount, count + 1).set(keySum, sum.add(BigInteger.valueOf(value)))
    }

    /**
     * generates the json message (including the random number)
     * save the minimum/the maximum and the sum
     * @param  the Index of the sensorType
     * @return a json formated string that will be the body of the message
     */
    ///json generator
    def generateJson(session: Session, sensorIndex: Int): Session = {
      val value: Long = generateNum();
      var jsonMsg = """{"id":"""" + generateId() + """",
						"timestamp":"""" + formatter.format(Instant.now()) + """",
						"sensorType":""" + (sensorIndex + 1) + """,
						"value":""" + value + """ }"""
      var session2 = min(session, "partialMin", value);
      session2 = max(session2, "partialMax", value);
      session2 = sum(session2, "partialCount", "partialSum", value);
      session2 = min(session2, "totalMin", value);
      session2 = max(session2, "totalMax", value);
      session2 = sum(session2, "totalCount", "totalSum", value);
      session2.set("json", jsonMsg)
    }

    def prepareQuerySynthese(session: Session, startTimeName: String): Session = {
      val startTime: Long = session(startTimeName).as[Long]
      //time the package of messages finished sending...
      val endTime: Long = Calendar.getInstance().getTimeInMillis()
      //duration that took the package to send all the messages
      val duration = endTime - startTime
      //Put parameters in session for next synthesis request
      session.set("paramDuration", "" + (1 + (duration / 1000)).toInt).set("paramTimestamp", formatter.format(Instant.ofEpochMilli(startTime)))
    }

    def querySynthese(sensorIndex: Int) = http("SynthesisSensor" + sensorIndex)
      .get("http://" + hostIp + "/messages/synthesis")
      .queryParam("duration", "${paramDuration}")
      .queryParam("timestamp", "${paramTimestamp}")
      .check(bodyString.saveAs("synthese"))
      .check(status.is(200))

    def validateSynthese(session: Session, sensorIndex: Int, prefix: String): Session = {
      val synthesisJson: List[Map[String, Any]] = scala.util.parsing.json.JSON.parseFull(session("synthese").as[String]).get.asInstanceOf[List[Map[String, Any]]]
      val map: Map[Int, Map[String, Any]] = synthesisJson.map(e => (e.get("sensorType").get.asInstanceOf[Number].intValue(), e))(breakOut)
      val remoteMin: Long = map(sensorIndex + 1).getOrElse("minValue", 0).asInstanceOf[Number].longValue
      val remoteMax: Long = map(sensorIndex + 1).getOrElse("maxValue", 0).asInstanceOf[Number].longValue
      val remoteAvg: BigDecimal = map(sensorIndex + 1).getOrElse("mediumValue", 0).asInstanceOf[BigDecimal]

      val localMin: Long = session(prefix + "Min").asOption[Long].getOrElse(0)
      val localMax: Long = session(prefix + "Max").asOption[Long].getOrElse(0)
      val localCount: Int = session(prefix + "Count").asOption[Int].getOrElse(1)
      val localSum: BigInteger = session(prefix + "Sum").asOption[BigInteger].getOrElse(BigInteger.valueOf(0))
      val localAvg: BigDecimal = (BigDecimal(localSum) / BigDecimal(localCount)).setScale(2, BigDecimal.RoundingMode.HALF_DOWN)

      if (remoteMin != localMin || remoteMax != localMax || remoteAvg != localAvg) {
        throw new java.lang.AssertionError("mauvaise synthese : " + (remoteMin != localMin) + " " + (remoteMax != localMax) + " " + (remoteAvg != localAvg) + " " + remoteMin + " " + localMin + " " + remoteMax + " " + localMax + " " + remoteAvg + " " + localAvg)
      }
      session.remove(prefix + "Min").remove(prefix + "Max").remove(prefix + "Count").remove(prefix + "Sum")
    }

    def SendMsgsAndCHeckSynthesis(sensorIndex: Int) = exec(session => {
      session.set("startTime", Calendar.getInstance().getTimeInMillis())
    }).repeat(numOfPackages) {
      exec(session => {
        session.set("startTimePackage", Calendar.getInstance().getTimeInMillis())
      })
        //send a package of messages and pause to round the time to seconds
        .repeat(msgPackage) {
          exec(session => {
            generateJson(session, sensorIndex)
          }).
            exec(http("request_" + sensorIndex)
              .post(url)
              .body(StringBody("${json}")).asJSON
              .check(status.is(200)))
        }
        //get the parameters of the synthesis call
        .exec(session =>
          prepareQuerySynthese(session, "startTimePackage"))
        //call and get the synthesis corresponding to the duration and timestamp
        .exec(querySynthese(sensorIndex))
        //check that the synthesis is correct
        .exec(session => validateSynthese(session, sensorIndex, "partial"))
    } //get the parameters of the synthesis call
      .exec(session =>
        prepareQuerySynthese(session, "startTime"))
      //call and get the synthesis corresponding to the duration and timestamp
      .exec(querySynthese(sensorIndex))
      //check that the synthesis is correct
      .exec(session => validateSynthese(session, sensorIndex, "total"))
  }

  val populationBuilders: List[PopulationBuilder] = List.tabulate(sensors)(i => scenario("injecteur " + i).exec(ScenarioBuilder.SendMsgsAndCHeckSynthesis(i)).inject(atOnceUsers(1)))

  /**
   * we run the scenarios and assert that 100%
   * of messages were received
   */
  setUp(populationBuilders)
    .protocols(httpProtocol)
    .assertions(global.successfulRequests.percent.is(100))

  /**
   * This Bloc of code runs after the simulation
   * the end time of the simulation
   * it verifies that the total  synthesis is correct
   * and sends the results to the leaderBoard
   */

  after {
    //end Time
    simulationEndTime = System.nanoTime()

    println("la simulation est finie traitement en cours...")

    //total time of the simulation in nanoseconds
    val timeOfSimulation = simulationEndTime - simulationStartTime

    println("Temp d'execution:" + timeOfSimulation + " Equipe:" + teamName + "participant:" + teamMembers + " rattachement:" + teameLocation)

    val password = scala.io.StdIn.readLine("entrez le mot de passe pour valider le résultat?: ")

    //the leader board is up
    val urlLeaderBoard = "http://concoursiot.northeurope.cloudapp.azure.com/resultsAAA"

    val post = new HttpPost(urlLeaderBoard)
    val client = HttpClientBuilder.create().build()

    post.setEntity(new StringEntity("""{"teamName":"""" + teamName + """",
                					    "time":"""" + timeOfSimulation + """",
                					    "location":"""" + teameLocation + """",
                					    "teamMembers":"""" + teamMembers + """",
                					    "password":"""" + password + """"}"""))

    val response = client.execute(post)

    if (response.getStatusLine().getStatusCode() == 200) {
      println("votre résultat est envoyé, n'oubliez pas de regarder le leaderBoard pour voir votre classement...")
    } else {
      println("L'envoi a échoué!!! le résultat n'a pas été envoyé au leaderboard!!!")
    }
  }
}
