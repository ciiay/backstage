/*
 * Copyright 2023 The Backstage Authors
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

import { EventParams, EventsService } from '@backstage/plugin-events-node';
import { SignalPayload } from '@backstage/plugin-signals-node';
import crypto from 'node:crypto';
import { RawData, WebSocket } from 'ws';
import { randomUUID as uuid } from 'node:crypto';
import { JsonObject } from '@backstage/types';
import {
  BackstageUserInfo,
  LifecycleService,
  LoggerService,
} from '@backstage/backend-plugin-api';
import { Config } from '@backstage/config';

const DEFAULT_CHANNELS = ['notifications', 'user-settings'];
const DEFAULT_MAX_CONNECTIONS_PER_USER = 5;
const DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION = 10;
const DEFAULT_MAX_MESSAGE_BYTES = 64 * 1024;

/**
 * @internal
 */
export type SignalConnection = {
  id: string;
  user: string;
  ws: WebSocket;
  ownershipEntityRefs: string[];
  subscriptions: Set<string>;
  isAlive: boolean;
};

/**
 * @internal
 */
export type SignalManagerOptions = {
  events: EventsService;
  config: Config;
  logger: LoggerService;
  lifecycle: LifecycleService;
};

/** @internal */
export class SignalManager {
  private connections: Map<string, SignalConnection> = new Map<
    string,
    SignalConnection
  >();
  private events: EventsService;
  private logger: LoggerService;
  private pingInterval: ReturnType<typeof setInterval> | undefined;
  private readonly allowedChannels: Set<string>;
  private readonly maxConnectionsPerUser: number;
  private readonly maxSubscriptionsPerConnection: number;
  private readonly maxMessageBytes: number;

  static create(options: SignalManagerOptions) {
    return new SignalManager(options);
  }

  private constructor(options: SignalManagerOptions) {
    this.events = options.events;

    // Use a unique subscriber ID for each signals instance, in order to fan-out
    // all events to each signals instance. This ensures that events always
    // reach users in a scaled deployment.
    const id = `signals-${crypto.randomBytes(8).toString('hex')}`;
    this.logger = options.logger.child({ subscriberId: id });
    this.logger.info(`Signals manager is subscribing to signals events`);

    this.allowedChannels = new Set(
      options.config.getOptionalStringArray('signals.channels') ??
        DEFAULT_CHANNELS,
    );
    this.maxConnectionsPerUser =
      options.config.getOptionalNumber('signals.maxConnectionsPerUser') ??
      DEFAULT_MAX_CONNECTIONS_PER_USER;
    this.maxSubscriptionsPerConnection =
      options.config.getOptionalNumber(
        'signals.maxSubscriptionsPerConnection',
      ) ?? DEFAULT_MAX_SUBSCRIPTIONS_PER_CONNECTION;
    this.maxMessageBytes =
      options.config.getOptionalNumber('signals.maxMessageBytes') ??
      DEFAULT_MAX_MESSAGE_BYTES;

    this.events.subscribe({
      id,
      topics: ['signals'],
      onEvent: (params: EventParams) =>
        this.onEventBrokerEvent(params.eventPayload as SignalPayload),
    });

    options.lifecycle.addShutdownHook(() => this.onShutdown());
  }

  private ping() {
    this.connections.forEach(conn => {
      if (!conn.isAlive) {
        this.logger.debug(`Connection ${conn.id} is not alive, terminating`);
        conn.ws.terminate();
        return;
      }

      conn.isAlive = false;
      conn.ws.ping();
    });
  }

  private onShutdown() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
    }

    // TODO: Unsubscribe from events?

    this.connections.forEach(conn => {
      conn.ws.terminate();
    });
    this.connections.clear();
  }

  private countConnectionsForUser(userEntityRef: string): number {
    let count = 0;
    for (const conn of this.connections.values()) {
      if (conn.user === userEntityRef) {
        count += 1;
      }
    }
    return count;
  }

  private sendError(
    connection: SignalConnection,
    payload: {
      action: string;
      channel?: string;
      error: string;
    },
  ) {
    if (connection.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    connection.ws.send(
      JSON.stringify({
        type: 'error',
        ...payload,
      }),
    );
  }

  addConnection(ws: WebSocket, identity: BackstageUserInfo) {
    if (
      this.countConnectionsForUser(identity.userEntityRef) >=
      this.maxConnectionsPerUser
    ) {
      this.logger.warn('WebSocket connection rejected: too many connections', {
        userEntityRef: identity.userEntityRef,
        maxConnectionsPerUser: this.maxConnectionsPerUser,
      });
      ws.close();
      ws.terminate();
      return;
    }

    // Start pinging on first connection
    if (!this.pingInterval) {
      this.pingInterval = setInterval(() => this.ping(), 30000);
    }

    const id = uuid();
    const conn = {
      id,
      user: identity.userEntityRef,
      ws,
      ownershipEntityRefs: identity.ownershipEntityRefs,
      subscriptions: new Set<string>(),
      isAlive: true,
    };

    this.connections.set(id, conn);

    this.logger.debug(`Connection ${id} connected`);
    ws.on('error', (err: Error) => {
      this.logger.error(
        `Error occurred with connection ${id}: ${err}, closing connection`,
      );
      ws.terminate();
      this.connections.delete(id);
    });

    ws.on('close', (code: number, reason: Buffer) => {
      this.logger.debug(
        `Connection ${id} closed with code ${code}, reason: ${reason}`,
      );
      ws.terminate();
      this.connections.delete(id);
    });

    ws.on('ping', () => {
      conn.isAlive = true;
      ws.pong();
    });

    ws.on('pong', () => {
      conn.isAlive = true;
    });

    ws.on('message', (data: RawData, isBinary: boolean) => {
      this.logger.debug(`Received message from connection ${id}: ${data}`);
      if (isBinary) {
        return;
      }

      const raw = data.toString();
      if (Buffer.byteLength(raw, 'utf8') > this.maxMessageBytes) {
        this.logger.info('Signal message rejected: too large', {
          userEntityRef: conn.user,
          connectionId: conn.id,
          maxMessageBytes: this.maxMessageBytes,
        });
        this.sendError(conn, {
          action: 'message',
          error: 'message_too_large',
        });
        return;
      }

      try {
        const json = JSON.parse(raw) as JsonObject;
        this.handleMessage(conn, json);
      } catch (err: any) {
        this.logger.error(
          `Invalid message received from connection ${id}: ${err}`,
        );
      }
    });
  }

  private handleMessage(connection: SignalConnection, message: JsonObject) {
    if (message.action === 'subscribe' && message.channel) {
      const channel = message.channel as string;

      if (!this.allowedChannels.has(channel)) {
        this.logger.info('Signal subscription denied', {
          userEntityRef: connection.user,
          channel,
          connectionId: connection.id,
        });
        this.sendError(connection, {
          action: 'subscribe',
          channel,
          error: 'not_allowed',
        });
        return;
      }

      if (connection.subscriptions.size >= this.maxSubscriptionsPerConnection) {
        this.logger.info('Signal subscription denied: too many subscriptions', {
          userEntityRef: connection.user,
          channel,
          connectionId: connection.id,
          maxSubscriptionsPerConnection: this.maxSubscriptionsPerConnection,
        });
        this.sendError(connection, {
          action: 'subscribe',
          channel,
          error: 'too_many_subscriptions',
        });
        return;
      }

      this.logger.debug(`Connection ${connection.id} subscribed to ${channel}`);
      connection.subscriptions.add(channel);
    } else if (message.action === 'unsubscribe' && message.channel) {
      this.logger.debug(
        `Connection ${connection.id} unsubscribed from ${message.channel}`,
      );
      connection.subscriptions.delete(message.channel as string);
    }
  }

  private async onEventBrokerEvent(eventPayload: SignalPayload): Promise<void> {
    if (!eventPayload.channel || !eventPayload.message) {
      return;
    }

    const { channel, recipients, message } = eventPayload;
    const jsonMessage = JSON.stringify({ channel, message });
    let users: string[] = [];
    if (recipients.type === 'user') {
      users = Array.isArray(recipients.entityRef)
        ? recipients.entityRef
        : [recipients.entityRef];
    }

    // Actual websocket message sending
    this.connections.forEach(conn => {
      if (!conn.subscriptions.has(channel)) {
        return;
      }

      // Sending to all users can be done with broadcast
      if (
        recipients.type !== 'broadcast' &&
        !conn.ownershipEntityRefs.some((ref: string) => users.includes(ref))
      ) {
        return;
      }

      if (conn.ws.readyState !== WebSocket.OPEN) {
        return;
      }

      conn.ws.send(jsonMessage, err => {
        if (err) {
          this.logger.error(`Failed to send message to ${conn.id}: ${err}`);
        }
      });
    });
  }
}
