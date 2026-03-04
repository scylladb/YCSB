/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
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

import org.testng.annotations.Test;

import java.io.InputStream;
import java.lang.reflect.Method;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;

public class TestBuildInfo {

  @Test
  public void singletonIsNeverNull() {
    assertNotNull(BuildInfo.getInstance());
  }

  @Test
  public void fieldsAreNonEmpty() {
    var info = BuildInfo.getInstance();
    // Values must be present (non-blank). version, gitHash and buildDate must
    // always resolve to real build-time data; gitTag is legitimately UNKNOWN
    // when HEAD has no tag pointing at it, so we only require non-blank there.
    assertFalse(info.version().isBlank(),   "version should not be blank");
    assertFalse(info.gitHash().isBlank(),   "gitHash should not be blank");
    assertFalse(info.gitTag().isBlank(),    "gitTag should not be blank");
    assertFalse(info.buildDate().isBlank(), "buildDate should not be blank");

    assertNotEquals(info.version(),   BuildInfo.UNKNOWN,
        "version should not be the UNKNOWN sentinel");
    assertNotEquals(info.gitHash(),   BuildInfo.UNKNOWN,
        "gitHash should not be the UNKNOWN sentinel — check git.commit.id.abbrev resolution at build time");
    // gitTag may legitimately be UNKNOWN when no tag points at HEAD — not asserted here.
    assertNotEquals(info.buildDate(), BuildInfo.UNKNOWN,
        "buildDate should not be the UNKNOWN sentinel");
  }

  @Test
  public void toStringContainsVersion() {
    var info = BuildInfo.getInstance();
    String s = info.toString();
    assertTrue(s.startsWith("{"));
    assertTrue(s.endsWith("}"));
    assertTrue(s.contains(info.version()));
    assertTrue(s.contains(info.gitHash()));
    assertTrue(s.contains(info.gitTag()));
    assertTrue(s.contains(info.buildDate()));
  }

  /**
   * Exercises the fallback code path in {@link BuildInfo} when
   * {@code version.properties} is absent from the classloader.
   *
   * <p>Uses a plain delegation-based {@link ClassLoader} subclass so that it
   * works on Java 9+ module-system loaders, which no longer extend
   * {@link java.net.URLClassLoader}.</p>
   */
  @Test
  public void unknownFallbackWhenPropertiesMissing() throws Exception {
    final ClassLoader ctx = Thread.currentThread().getContextClassLoader();

    ClassLoader isolatedLoader = new ClassLoader(ClassLoader.getPlatformClassLoader()) {
      @Override
      protected Class<?> findClass(String name) throws ClassNotFoundException {
        String path = name.replace('.', '/') + ".class";
        try (InputStream in = ctx.getResourceAsStream(path)) {
          if (in == null) {
            throw new ClassNotFoundException(name);
          }
          byte[] bytes = in.readAllBytes();
          return defineClass(name, bytes, 0, bytes.length);
        } catch (java.io.IOException e) {
          throw new ClassNotFoundException(name, e);
        }
      }

      @Override
      public InputStream getResourceAsStream(String name) {
        if ("version.properties".equals(name)) {
          return null;   // simulate missing resource — triggers the fallback path
        }
        return ctx.getResourceAsStream(name);
      }
    };

    Class<?> buildInfoClass = Class.forName("site.ycsb.BuildInfo", true, isolatedLoader);

    Method getInstance = buildInfoClass.getMethod("getInstance");
    Object instance = getInstance.invoke(null);

    // Read UNKNOWN from the freshly-loaded class to avoid cross-loader string mismatch.
    String unknown = (String) buildInfoClass.getField("UNKNOWN").get(null);

    for (String methodName : new String[]{"version", "gitHash", "gitTag", "buildDate"}) {
      Object value = buildInfoClass.getMethod(methodName).invoke(instance);
      assertEquals(value, unknown,
          methodName + "() should return UNKNOWN when version.properties is missing");
    }
  }
}
