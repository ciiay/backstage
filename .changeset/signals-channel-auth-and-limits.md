---
'@backstage/plugin-signals-backend': patch
---

Signal channel subscriptions are now limited to an allowlisted set of channels (defaults to `notifications` and `user-settings`). Unknown channels are denied with a structured WebSocket error. Basic resource limits also apply for connections per user, subscriptions per connection, and inbound message size. Custom channels can be enabled with the `signals.channels` configuration.
