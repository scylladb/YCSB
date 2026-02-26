/**
 * Copyright (c) 2024 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb;

import org.testng.annotations.Test;

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
    assertFalse(info.version().isBlank());
    assertFalse(info.gitHash().isBlank());
    assertFalse(info.gitTag().isBlank());
    assertFalse(info.buildDate().isBlank());
  }

  @Test
  public void toStringContainsVersion() {
    var info = BuildInfo.getInstance();
    String s = info.toString();
    assertTrue(s.startsWith("YCSB "));
    assertTrue(s.contains(info.version()));
    assertTrue(s.contains(info.gitHash()));
    assertTrue(s.contains(info.gitTag()));
    assertTrue(s.contains(info.buildDate()));
  }

  @Test
  public void unknownFallbackWhenPropertiesMissing() {
    // BuildInfo.load() falls back to UNKNOWN for any missing key; verify the constant itself
    assertNotNull(BuildInfo.UNKNOWN);
    assertFalse(BuildInfo.UNKNOWN.isBlank());
  }
}

