# signals

Welcome to the signals backend plugin!

Signals plugin allows backend plugins to publish messages to frontend plugins.

## Getting started

To install this signals backend plugin, please refer the [Getting Started](https://backstage.io/docs/notifications) Backstage Notifications and Signals documentation section.

## Configuration

```yaml
signals:
  # Channels clients may subscribe to (unknown channels are denied).
  # Defaults to notifications and user-settings.
  channels:
    - notifications
    - user-settings
  # Optional resource limits
  maxConnectionsPerUser: 5
  maxSubscriptionsPerConnection: 10
  maxMessageBytes: 65536
```
