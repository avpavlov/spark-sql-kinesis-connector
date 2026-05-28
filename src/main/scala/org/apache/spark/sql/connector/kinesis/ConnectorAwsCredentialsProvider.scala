/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.connector.kinesis

import java.net.URI
import java.nio.file.{Files, Paths}

import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sts.StsClient
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest

import org.apache.spark.internal.Logging

/**
 * Serializable interface providing a method executors can call to obtain an
 * AWSCredentialsProvider instance for authenticating to AWS services.
 */
sealed trait ConnectorAwsCredentialsProvider extends Serializable {
  def provider: AwsCredentialsProvider
  def close(): Unit
}

case class ConnectorDefaultCredentialsProvider() extends ConnectorAwsCredentialsProvider {

  private val CREDENTIALS_FILE = "/home/hadoop/.aws/credentials"
  private val CACHE_DURATION_MS = 10 * 60 * 1000L // 10 minutes

  private val cachedProvider = new FileCredentialsProvider(CREDENTIALS_FILE, CACHE_DURATION_MS)

  override def provider: AwsCredentialsProvider = cachedProvider

  override def close(): Unit = {}
}

/**
 * Reads AWS credentials from a file, caches them for a configurable duration,
 * then re-reads. Failed reads do not clear previously cached credentials.
 */
class FileCredentialsProvider(
    filePath: String,
    cacheDurationMs: Long
) extends AwsCredentialsProvider with Logging with Serializable {

  @volatile private var cachedCredentials: AwsCredentials = tryReadFile().getOrElse(
    throw new RuntimeException(s"Failed to read credentials from $filePath on initialization")
  )
  @volatile private var lastReadTime: Long = System.currentTimeMillis()

  override def resolveCredentials(): AwsCredentials = {
    val now = System.currentTimeMillis()
    if ((now - lastReadTime) > cacheDurationMs) {
      tryReadFile() match {
        case Some(creds) =>
          cachedCredentials = creds
          lastReadTime = now
        case None =>
          logWarning(s"Failed to read credentials from $filePath, using cached credentials")
      }
    }
    cachedCredentials
  }

  private def tryReadFile(): Option[AwsCredentials] = {
    try {
      val path = Paths.get(filePath)
      if (!Files.exists(path)) {
        logWarning(s"Credentials file does not exist: $filePath")
        return None
      }
      val lines = new String(Files.readAllBytes(path), "UTF-8").split("\n")
      var accessKey: String = null
      var secretKey: String = null
      var sessionToken: String = null
      var expiration: String = null

      lines.foreach { line =>
        val trimmed = line.trim
        if (trimmed.startsWith("aws_access_key_id")) {
          accessKey = trimmed.split("=", 2).last.trim
        } else if (trimmed.startsWith("aws_secret_access_key")) {
          secretKey = trimmed.split("=", 2).last.trim
        } else if (trimmed.startsWith("aws_session_token")) {
          sessionToken = trimmed.split("=", 2).last.trim
        } else if (trimmed.startsWith("expiration")) {
          expiration = trimmed.split("=", 2).last.trim
        }
      }

      if (accessKey == null || secretKey == null) {
        logWarning(s"Credentials file $filePath missing access key or secret key")
        None
      } else if (sessionToken != null) {
        logInfo(s"Credentials reloaded from $filePath, expiration=$expiration")
        Some(AwsSessionCredentials.create(accessKey, secretKey, sessionToken))
      } else {
        logInfo(s"Credentials reloaded from $filePath (basic credentials, no expiration)")
        Some(software.amazon.awssdk.auth.credentials.AwsBasicCredentials.create(accessKey, secretKey))
      }
    } catch {
      case e: Exception =>
        logWarning(s"Error reading credentials file $filePath: ${e.getMessage}")
        None
    }
  }
}

case class ConnectorSTSCredentialsProvider(
                           stsRoleArn: String,
                           stsSessionName: String,
                           region: String,
                           credentialsProvider: ConnectorDefaultCredentialsProvider,
                           stsEndpoint: Option[String] = None,
                                          )
  extends ConnectorAwsCredentialsProvider  {

  private var providerOpt: Option[StsAssumeRoleCredentialsProvider] = None
  private var stsClientOpt: Option[StsClient] = None
  def provider: AwsCredentialsProvider = {
    if (providerOpt.isEmpty) {
      val stsClientBuilder = StsClient.builder
        .credentialsProvider(credentialsProvider.provider)
        .httpClientBuilder(ApacheHttpClient.builder())

      stsClientOpt = Some(stsEndpoint
        .map(endpoint =>
          stsClientBuilder.endpointOverride(new URI(endpoint))
        )
        .getOrElse(
          stsClientBuilder.region(Region.of(region))
        )
        .build())

      val assumeRoleRequest = AssumeRoleRequest.builder
        .roleArn(stsRoleArn)
        .roleSessionName(stsSessionName)
        .build

      val stsAssumeRoleCredentialsProvider = StsAssumeRoleCredentialsProvider.builder
        .stsClient(stsClientOpt.get)
        .refreshRequest(assumeRoleRequest)
        .asyncCredentialUpdateEnabled(true)
        .build

      providerOpt = Some(stsAssumeRoleCredentialsProvider)
    }

    providerOpt.get
  }

  override def close(): Unit = {
    tryAndIgnoreError("close sts client") { stsClientOpt.foreach(_.close())}
    tryAndIgnoreError("close sts provider") { providerOpt.foreach(_.close()) }
  }
}

class Builder {
  private var stsRoleArn: Option[String] = None
  private var stsSessionName: Option[String] = None
  private var stsRegion: Option[String] = None
  private var stsEndpoint: Option[String] = None

  def stsCredentials(roleArn: Option[String],
                     sessionName: Option[String],
                     region: String,
                     endpoint: Option[String] = None): Builder = {
    stsRoleArn = roleArn
    stsSessionName = sessionName
    stsRegion = Some(region)
    stsEndpoint = endpoint
    this
  }

  def build(): ConnectorAwsCredentialsProvider = {
    val defaultProvider = ConnectorDefaultCredentialsProvider()

    stsRoleArn
      .map { _ =>
        ConnectorSTSCredentialsProvider(
          stsRoleArn.get,
          stsSessionName.get,
          stsRegion.get,
          defaultProvider,
          stsEndpoint
        )
      }
      .getOrElse(defaultProvider)
  }
}

object ConnectorAwsCredentialsProvider {
  def builder: Builder = new Builder
}

