/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.base;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommandUtilTest {
  @Test
  public void testUriBasename() {
    test("c", "c", ImmutableMap.<String, String>of());
    test("c", "/a/b/c", ImmutableMap.of("FOO", "1"));
    test("foo.zip", "hdfs://twitter.com/path/foo.zip", ImmutableMap.of("PATH", "/bin:/usr/bin"));
  }

  @Test
  public void testExecutorOnlyCommand() {
    CommandInfo cmd = CommandUtil.create("test/executor", null);
    assertEquals("./executor", cmd.getValue());
    assertEquals("test/executor", cmd.getUris(0).getValue());
  }

  @Test
  public void testWrapperOnlyCommand() {
    CommandInfo cmd = CommandUtil.create(null, "test/wrapper");
    assertEquals("./wrapper", cmd.getValue());
    assertEquals("test/wrapper", cmd.getUris(0).getValue());
  }

  @Test
  public void testWrapperAndExecutorCommand() {
    CommandInfo cmd = CommandUtil.create("test/executor", "test/wrapper");
    assertEquals("./wrapper", cmd.getValue());
    assertEquals("test/wrapper", cmd.getUris(0).getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadParameters() {
    CommandUtil.create(null, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadUri() {
    CommandUtil.create("a/b/c/", null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyUri() {
    CommandUtil.create("", null);
  }

  private void test(String basename, String uri, Map<String, String> env) {
    CommandInfo expectedCommand = CommandInfo.newBuilder()
        .addUris(URI.newBuilder().setValue(uri).setExecutable(true))
        .setValue("./" + basename)
        .build();
    assertEquals(expectedCommand, CommandUtil.create(uri, null));
  }
}
