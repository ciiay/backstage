/*
 * Copyright 2024 The Backstage Authors
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

import { WebSocket } from 'ws';
import { EventsServiceSubscribeOptions } from '@backstage/plugin-events-node';
import { SignalManager } from './SignalManager';
import { mockServices } from '@backstage/backend-test-utils';
import { JsonObject } from '@backstage/types';

class MockWebSocket {
  closed: boolean = false;
  readyState: number = WebSocket.OPEN;
  callbacks: Map<string | symbol, (this: WebSocket, ...args: any[]) => void> =
    new Map();
  data: any[] = [];

  close(_: number, __: string | Buffer): void {
    this.readyState = WebSocket.CLOSED;
    this.closed = true;
  }

  terminate(): void {
    this.readyState = WebSocket.CLOSED;
    this.closed = true;
  }

  on(
    event: string | symbol,
    listener: (this: WebSocket, ...args: any[]) => void,
  ) {
    this.callbacks.set(event, listener);
    return this;
  }

  // @ts-ignore
  send(data: any, _?: (err?: Error) => void): void {
    this.data.push(data);
  }

  trigger(event: string | symbol, ...args: any[]): void {
    const cb = this.callbacks.get(event);
    if (!cb) {
      throw new Error(`No callback for ${event.toString()}`);
    }
    // @ts-ignore
    cb(...args);
  }
}

const guestIdentity = {
  userEntityRef: 'user:default/guest',
  ownershipEntityRefs: ['user:default/guest'],
};

const johnIdentity = {
  userEntityRef: 'user:default/john.doe',
  ownershipEntityRefs: ['user:default/john.doe'],
};

function createManager(config: JsonObject = {}) {
  let onEvent: Function = async () => {};
  const shutdownHooks: Function[] = [];

  const mockEvents = {
    publish: async () => {},
    subscribe: async (subscriber: EventsServiceSubscribeOptions) => {
      onEvent = subscriber.onEvent;
    },
  };

  const manager = SignalManager.create({
    events: mockEvents,
    logger: mockServices.logger.mock(),
    config: mockServices.rootConfig({
      data: {
        signals: {
          channels: ['notifications', 'user-settings', 'test'],
          ...config,
        },
      },
    }),
    lifecycle: mockServices.lifecycle.mock({
      addShutdownHook: (hook: Function) => shutdownHooks.push(hook),
    }),
  });

  return {
    manager,
    get onEvent() {
      return onEvent;
    },
    shutdown() {
      shutdownHooks.forEach(hook => hook());
    },
  };
}

describe('SignalManager', () => {
  const managers: Array<{ shutdown: () => void }> = [];

  afterEach(() => {
    while (managers.length) {
      managers.pop()?.shutdown();
    }
  });

  function managerWith(config: JsonObject = {}) {
    const ctx = createManager(config);
    managers.push(ctx);
    return ctx;
  }

  it('should close all connections when server is closed', () => {
    const { manager, shutdown } = managerWith();
    const ws = new MockWebSocket();
    manager.addConnection(ws as unknown as WebSocket, guestIdentity);
    shutdown();
    expect(ws.closed).toBeTruthy();
  });

  it('should close connection on error', () => {
    const { manager } = managerWith();
    const ws = new MockWebSocket();
    manager.addConnection(ws as unknown as WebSocket, guestIdentity);

    ws.trigger('error', new Error('error'));
    expect(ws.closed).toBeTruthy();
  });

  it('should allow subscribing and unsubscribing to events', async () => {
    const ctx = managerWith();
    const ws = new MockWebSocket();
    ctx.manager.addConnection(ws as unknown as WebSocket, guestIdentity);

    ws.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'test' }),
      false,
    );

    await ctx.onEvent({
      topic: 'signals',
      eventPayload: {
        recipients: { type: 'broadcast' },
        channel: 'test',
        message: { msg: 'test' },
      },
    });

    expect(ws.data.length).toEqual(1);
    expect(ws.data[0]).toEqual(
      JSON.stringify({ channel: 'test', message: { msg: 'test' } }),
    );

    ws.trigger(
      'message',
      JSON.stringify({ action: 'unsubscribe', channel: 'test' }),
      false,
    );

    await ctx.onEvent({
      topic: 'signals',
      eventPayload: {
        recipients: { type: 'broadcast' },
        channel: 'test',
        message: { msg: 'test' },
      },
    });

    expect(ws.data.length).toEqual(1);
  });

  it('should only send to users from identity', async () => {
    const ctx = managerWith();
    const ws1 = new MockWebSocket();
    ctx.manager.addConnection(ws1 as unknown as WebSocket, guestIdentity);

    const ws2 = new MockWebSocket();
    ctx.manager.addConnection(ws2 as unknown as WebSocket, johnIdentity);

    const ws3 = new MockWebSocket();
    ctx.manager.addConnection(ws3 as unknown as WebSocket, johnIdentity);

    ws1.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'test' }),
      false,
    );

    ws2.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'test' }),
      false,
    );

    await ctx.onEvent({
      topic: 'signals',
      eventPayload: {
        recipients: { type: 'user', entityRef: 'user:default/john.doe' },
        channel: 'test',
        message: { msg: 'test' },
      },
    });

    expect(ws1.data.length).toEqual(0);
    expect(ws3.data.length).toEqual(0);
    expect(ws2.data.length).toEqual(1);
    expect(ws2.data[0]).toEqual(
      JSON.stringify({ channel: 'test', message: { msg: 'test' } }),
    );
  });

  it('should deny subscriptions to unknown channels', async () => {
    const ctx = managerWith();
    const ws = new MockWebSocket();
    ctx.manager.addConnection(ws as unknown as WebSocket, johnIdentity);

    ws.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'secret-channel' }),
      false,
    );

    expect(ws.data).toEqual([
      JSON.stringify({
        type: 'error',
        action: 'subscribe',
        channel: 'secret-channel',
        error: 'not_allowed',
      }),
    ]);
    ws.data = [];

    await ctx.onEvent({
      topic: 'signals',
      eventPayload: {
        recipients: { type: 'broadcast' },
        channel: 'secret-channel',
        message: { msg: 'leak' },
      },
    });
    expect(ws.data.length).toEqual(0);
  });

  it('should reject excess connections for the same user', () => {
    const { manager } = managerWith({ maxConnectionsPerUser: 1 });
    const ws1 = new MockWebSocket();
    const ws2 = new MockWebSocket();

    manager.addConnection(ws1 as unknown as WebSocket, johnIdentity);
    manager.addConnection(ws2 as unknown as WebSocket, johnIdentity);

    expect(ws1.closed).toBeFalsy();
    expect(ws2.closed).toBeTruthy();
  });

  it('should reject excess subscriptions on a connection', () => {
    const { manager } = managerWith({
      channels: ['a', 'b'],
      maxSubscriptionsPerConnection: 1,
    });
    const ws = new MockWebSocket();
    manager.addConnection(ws as unknown as WebSocket, johnIdentity);

    ws.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'a' }),
      false,
    );
    ws.trigger(
      'message',
      JSON.stringify({ action: 'subscribe', channel: 'b' }),
      false,
    );

    expect(ws.data).toEqual([
      JSON.stringify({
        type: 'error',
        action: 'subscribe',
        channel: 'b',
        error: 'too_many_subscriptions',
      }),
    ]);
  });

  it('should reject oversized messages', () => {
    const { manager } = managerWith({ maxMessageBytes: 16 });
    const ws = new MockWebSocket();
    manager.addConnection(ws as unknown as WebSocket, johnIdentity);

    ws.trigger('message', 'x'.repeat(64), false);

    expect(ws.data).toEqual([
      JSON.stringify({
        type: 'error',
        action: 'message',
        error: 'message_too_large',
      }),
    ]);
  });
});
