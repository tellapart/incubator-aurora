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

import com.twitter.common.base.MorePreconditions;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;

/**
 * Utility class for constructing {@link CommandInfo} objects given an executor URI.
 */
public final class CommandUtil {

  private CommandUtil() {
    // Utility class.
  }

  private static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return uri;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");

      return basename;
    }
  }

  public static CommandInfo create(String executorUri, String wrapperUri) {
    CommandInfo.Builder builder = CommandInfo.newBuilder();
    create(executorUri, wrapperUri, "./", builder);
    return builder.build();
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri URI to the executor.
   * @param wrapperUri URI to the executor wrapper
   * @param basePath The base path to the executor
   * @param builder A CommandBuilder to populate
   */
  public static void create(String executorUri, String wrapperUri, String basePath, CommandInfo.Builder builder) {
    String uriToAdd = null;

    if (wrapperUri != null) {
      if (executorUri != null) {
        builder.addArguments("--executorUri=" + executorUri);
      }
      uriToAdd = wrapperUri;
    }
    else if (wrapperUri == null && executorUri != null) {
      uriToAdd = executorUri;
    }
    else {
      throw new IllegalArgumentException("At least executorUri or wrapperUri must be non-null");
    }

    builder.setValue(basePath + uriBasename(uriToAdd));
    builder.addUris(URI.newBuilder().setValue(uriToAdd).setExecutable(true));
  }
}
