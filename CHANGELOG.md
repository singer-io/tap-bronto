# Changelog

## 1.0.0
  * Version bump for initial release

## 0.1.1
  * Set`[inbound|outbound]_activity.contactId` to nullable in the schema.

## 0.1.0
  * Refactor out suds and replace with zeep because of a memory leak [#5](https://github.com/singer-io/tap-bronto/pull/5)

## 0.0.6
  * Use metadata to discover `replication-method` during sync
