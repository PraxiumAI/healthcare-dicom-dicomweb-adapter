package com.google.cloud.healthcare.imaging.dicomadapter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/** Provides build metadata loaded from build-info.properties generated at build time. */
public final class BuildInfo {

  private static final String RESOURCE = "/build-info.properties";
  private static final Properties props = new Properties();

  static {
    try (InputStream in = BuildInfo.class.getResourceAsStream(RESOURCE)) {
      if (in != null) {
        props.load(in);
      }
    } catch (IOException ignored) {
    }
  }

  private BuildInfo() {}

  public static String get(String key, String defaultValue) {
    return props.getProperty(key, defaultValue);
  }

  public static String commit() {
    return get("commit", "unknown");
  }

  public static String shortCommit() {
    return get("shortCommit", "unknown");
  }

  public static String buildTime() {
    return get("buildTime", "unknown");
  }

  public static String version() {
    return get("version", "unknown");
  }
}


