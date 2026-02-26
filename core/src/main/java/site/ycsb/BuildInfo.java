/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Exposes build metadata baked in at compile time via Maven resource filtering.
 */
public final class BuildInfo {

  /**
   * Sentinel returned for any field that could not be resolved at build time.
   */
  public static final String UNKNOWN = "unknown";

  private final String version;
  private final String gitHash;
  private final String gitTag;
  private final String buildDate;
  private final String awsSdkVersion;
  private final String scyllaDriverVersion;

  private BuildInfo(
      final String ver,
      final String hash,
      final String tag,
      final String date,
      final String awsSdk,
      final String scyllaDriver) {
    this.version = ver;
    this.gitHash = hash;
    this.gitTag = tag;
    this.buildDate = date;
    this.awsSdkVersion = awsSdk;
    this.scyllaDriverVersion = scyllaDriver;
  }

  /**
   * Eagerly loaded singleton.
   */
  private static final BuildInfo INSTANCE = load();

  private static BuildInfo load() {
    ClassLoader cl = BuildInfo.class.getClassLoader();
    try (InputStream in = cl.getResourceAsStream("version.properties")) {
      if (in == null) {
        return new BuildInfo(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);
      }
      var props = new Properties();
      props.load(in);
      return new BuildInfo(
          resolve(props, "version"),
          resolve(props, "git.hash"),
          resolve(props, "git.tag"),
          resolve(props, "build.date"),
          resolve(props, "aws.sdk.version"),
          resolve(props, "scylla.driver.version")
      );
    } catch (IOException e) {
      return new BuildInfo(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);
    }
  }

  /**
   * Returns the property value, falling back to {@link #UNKNOWN} when the
   * key is absent, blank, or still contains an unresolved Maven placeholder.
   */
  private static String resolve(Properties props, String key) {
    String value = props.getProperty(key, UNKNOWN);
    if (value.isBlank() || value.startsWith("${")) {
      return UNKNOWN;
    }
    return value;
  }

  /**
   * Returns the singleton instance loaded from {@code version.properties}.
   *
   * @return the singleton instance.
   */
  public static BuildInfo getInstance() {
    return INSTANCE;
  }

  /**
   * Maven project version string, e.g. {@code 1.3.0-SNAPSHOT}.
   *
   * @return the project version.
   */
  public String version() {
    return version;
  }

  /**
   * Abbreviated git commit hash at build time.
   *
   * @return the git hash.
   */
  public String gitHash() {
    return gitHash;
  }

  /**
   * Nearest git tag at build time.
   *
   * @return the git tag.
   */
  public String gitTag() {
    return gitTag;
  }

  /**
   * ISO-8601 build timestamp.
   *
   * @return the build date.
   */
  public String buildDate() {
    return buildDate;
  }

  public String awsSdkVersion() {
    return awsSdkVersion;
  }

  public String scyllaDriverVersion() {
    return scyllaDriverVersion;
  }

  @Override
  public String toString() {
    return "{\"version\":\"" + version + "\""
        + ",\"git_hash\":\"" + gitHash + "\""
        + ",\"git_tag\":\"" + gitTag + "\""
        + ",\"build_date\":\"" + buildDate + "\""
        + ",\"aws_sdk_version\":\"" + awsSdkVersion + "\""
        + ",\"scylla_driver_version\":\"" + scyllaDriverVersion + "\""
        + "}";
  }
}

