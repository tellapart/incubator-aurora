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
package org.apache.aurora.auth;

import java.nio.charset.StandardCharsets;

import java.util.Set;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.inject.AbstractModule;

import org.apache.aurora.gen.SessionKey;

/**
 * An authentication module that uses an {@link IdentitySessionValidator}.
 */
public class IdentityAuthModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(IdentityAuthModule.class.getName());
  private static final Gson GSON = new Gson();

  private static class Identity {
    String identifier;
  }

  @Override
  protected void configure() {
    LOG.info("Using Identity authentication module.");
    bind(SessionValidator.class).to(IdentitySessionValidator.class);
    bind(CapabilityValidator.class).to(IdentityCapabilityValidator.class);
  }

  /**
   * Extracts the identity of the current user from a SessionKey.
   *
   * @param key A SessionKey,
   * @return A string representing the identity of the user.
   */
  private static String extractIdentity(SessionKey key) {
    // This try/catch is needed now because any operation not using a .aur file
    // will not inject the correct information into the session. Once we have a custom
    // client, this will no longer be an issue and we can change this to raise
    // an AuthFailedException.
    try {
      String rawData = new String(key.getData(), StandardCharsets.UTF_8);
      Identity identity = GSON.fromJson(rawData, Identity.class);
      return identity.identifier;
    } catch (JsonSyntaxException ex) {
      return "UNKNOWN";
    }
  }

  /**
   * A SessionValidator that simply returns the UTF-8 value of SessionKey.data as Identity.
   *
   * This is NOT useful for real authentication; it only serves to put the current username
   * into job create/update logs.
   */
  static class IdentitySessionValidator implements SessionValidator {
    @Override
    public SessionContext checkAuthenticated(final SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return new SessionContext() {
        @Override
        public String getIdentity() {
          return extractIdentity(key);
        }
      };
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }

  static class IdentityCapabilityValidator implements CapabilityValidator {
    @Override
    public SessionContext checkAuthorized(final SessionKey key, Capability capability, AuditCheck check)
        throws AuthFailedException {

      return new SessionContext() {
        @Override
        public String getIdentity() {
          return extractIdentity(key);
        }
      };
    }

    @Override
    public SessionContext checkAuthenticated(final SessionKey key, Set<String> targetRoles)
        throws AuthFailedException {

      return new SessionContext() {
        @Override
        public String getIdentity() {
          return extractIdentity(key);
        }
      };
    }

    @Override
    public String toString(SessionKey sessionKey) {
      return sessionKey.toString();
    }
  }
}
