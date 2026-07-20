/*
 * Copyright 2026 The Backstage Authors
 *
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

export interface Config {
  /**
   * Configuration options for the signals backend
   */
  signals?: {
    /**
     * Channel names that authenticated clients may subscribe to.
     * Unknown channels are denied. Defaults to `notifications` and
     * `user-settings`.
     */
    channels?: string[];
    /**
     * Maximum concurrent WebSocket connections per user entity ref.
     * Defaults to 5.
     */
    maxConnectionsPerUser?: number;
    /**
     * Maximum channel subscriptions per connection.
     * Defaults to 10.
     */
    maxSubscriptionsPerConnection?: number;
    /**
     * Maximum inbound WebSocket text message size in bytes.
     * Defaults to 65536 (64 KiB).
     */
    maxMessageBytes?: number;
  };
}
