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

import com.google.common.base.Objects;
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

  /**
   * Gets the last part of the path of a URI.
   *
   * @param uri URI to parse
   * @return The last segment of the URI.
   */
  public static String uriBasename(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return uri;
    } else {
      String basename = uri.substring(lastSlash + 1);
      MorePreconditions.checkNotBlank(basename, "URI must not end with a slash.");

      return basename;
    }
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri A URI to the executor
   * @param wrapperUri A URI to the wrapper
   * @return A populated CommandInfo with correct resources set and command set.
   */
  public static CommandInfo create(String executorUri, String wrapperUri) {
    return create(executorUri, wrapperUri, null, null, null).build();
  }

  /**
   * Creates a description of a command that will fetch and execute the given URI to an executor
   * binary.
   *
   * @param executorUri A optional URI to the executor
   * @param wrapperUri An optional URI to the wrapper
   * @param commandPrefix An option string to prefix the generated command with
   * @param commandSuffix An optional string to suffix the generated command with
   * @param extraArguments Extra command line arguments to add to the generated command.
   * @return A CommandInfo.Builder populated with resources and a command.
   */
  public static CommandInfo.Builder create(
      String executorUri,
      String wrapperUri,
      String commandPrefix,
      String commandSuffix,
      String extraArguments) {
    CommandInfo.Builder builder = CommandInfo.newBuilder();
    populate(executorUri, wrapperUri, "./", builder);
    String cmdLine = Objects.firstNonNull(commandPrefix, "")
        + builder.getValue()
        + Objects.firstNonNull(commandSuffix, "")
        + " " + Objects.firstNonNull(extraArguments, "");
    return builder
        .setValue(cmdLine.trim())
        .setShell(true);
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
  public static void populate(
      String executorUri,
      String wrapperUri,
      String basePath,
      CommandInfo.Builder builder) {
    String uriToAdd;

    if (wrapperUri != null) { //NOPMD - http://sourceforge.net/p/pmd/bugs/228/
      MorePreconditions.checkNotBlank(wrapperUri);
      uriToAdd = wrapperUri;
    } else if (executorUri != null) { //NOPMD - http://sourceforge.net/p/pmd/bugs/228/
      MorePreconditions.checkNotBlank(executorUri);
      uriToAdd = executorUri;
    } else {
      throw new IllegalArgumentException("At least executorUri or wrapperUri must be non-null");
    }

    builder.setValue(basePath + uriBasename(uriToAdd));
    builder.addUris(URI.newBuilder().setValue(uriToAdd).setExecutable(true));
  }
}
