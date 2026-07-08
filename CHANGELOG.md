### Version 1.8.1

* **md5 auth fix (regression in 1.8.0).** The salt is now hashed as raw bytes; 1.8.0 UTF-8-encoded it, failing ~15/16 of md5-auth attempts (SCRAM/trust unaffected).
* Pool: double `destroy()` no longer establishes two replacements; the heartbeat replenishes to `minConnections` (e.g. after a DB outage); a connection that dies during hand-off to a waiter (57P shutdown, socket error) is dropped and the waiter gets another; `PooledConnection` now declares `destroy()`.
* Substitution follows `standard_conforming_strings`: backslash escapes only inside `E'...'`; literal in `'...'`/`"..."`.
* `queryByList`/`executeByList`/`substituteByList`: `values` is now a non-nullable `List` (use `query`/`execute` when there are none).
* `decodeArray` documents its single-dimension/default-bound limitation and asserts on multidimensional or dimension-prefixed input (was: silently decoded garbage).

### Version 1.8.0

* `Connection.destroy()` added — physically closes a pooled connection so session state (e.g. a `SET`) won't leak to the next borrower. (Breaking for `Connection` implementers.)
* Pool fixes: external destroy re-establishes and serves waiters; a failed connection test no longer strands the connection in `testing`; a failed `start()` ends in `startFailed` and destroys what it established (was: stuck in `starting`, leaking sockets); socket leak on destroy of a `connecting` entry; `stop()` cancels the retry timer; dropped `_testConnection`'s unused `onTimeout` parameter.
* Substitution: `@params` in `--`/`/*...*/` comments are no longer substituted; dollar quotes match the exact `$tag$` terminator, incl. non-ASCII tags.
* Type converter: arrays with quoted elements (`timestamp[]`, `money[]`…) decode correctly; `json[]` elements decode to values (consistent with scalar `json`); typed `BigInt` no longer crashes.
* md5 auth hashes UTF-8 bytes (was UTF-16 code units — wrong for non-ASCII credentials).
* `close()` bounds the terminate-flush wait (5s) and destroys the socket even when Terminate throws.

### Version 1.7.2

* Connection-hang fixes: pending-queries leak on `close`/socket error/PG admin shutdown; `connectionTimeout` now bounds the handshake, not just the socket; `runInTransaction` rollback no longer hides the original exception; `_establishConnectionSafely`'s retry loop actually retries; `Pool.testConnections` retry condition was inverted.
* Smaller fixes: `Buffer.readUtf8String` maxSize, `_handleSocketError` parameter shadowing, `Settings.toUri` query string, `peConnectionFailed` 40004→4004, `ConnectionDecorator.runInTransaction` double-throw.
* `Message.message`/`ServerMessage.message`: nullable → non-nullable. Doc/style cleanup; deleted unused `lib/src/protocol.dart`.

### Version 1.7.0

* *BREAK CHANGE* The `values` paramter of `Connection.query` and `execute` must be `Map`. For `List` values (by-index), please use `queryByList` and `executeByList` instead.

### Version 1.6.2

* The `stats` parameter added to `onQuery` and `onExecute` to pass application-specific data for monitoring the connections, such as detecting abnormal access, such as a dead loop.
* The `onOpen` callback added to `PoolSettings` to prepare the `stats` paremeter mentioned above.

### Version 1.0.4

* `Pool.typeConverter` added.

### Version 1.0.3

* `runInTransaction` supports nested transactions.

### Version 1.0.2

* The `onExecute` and `onQuery` callback are added to `PoolSettings` to detect unexpected patterns, such as a missing `@` or low-performance statements.

### Version 1.0.1

* Fix #20: remove connections from the pool when detecting server restarted/crashed
* Fix #21: retry if failed to establish a connection

### Version 1.0.0

* Migrate to null safety
* Numeric and Decimal types are consider as `double`, instead of `BigInt`.
    * It means the precision is bounded by `double`.
* The `getConnectionName()` and `getDebugName()` arguments are simplifed as `String? connectionName` and `String? debugName`.
* `Settings.fromMap` and `Settings.toMap` are removed.
* The `mockSocketConnect` argument is removed.

### Version 0.7.7

* Rename `freeConnections` to `limitConnections`
* Add `limitTimeout` to control how long to wait when exceeding `limitConnections`

### Version 0.7.6

* If `freeConnections` is set, we'll wait up to 700ms, if number of connections exceeds [freeConnections].
It helps to reduce number of connections if there are a lot of short-lived connections.

### Version 0.7.4

* `Settings.onMaxConnection` introduced to monitor the usage of DB connections.

### Version 0.7.3

* `freeConnections` introduced to control maximal number of connections kept in a pool.

### Version 0.7.0

* **Breaking**: the substition with a Map instance won't treat number identifiers specially. For example, if `@0` is specified, it will consider the identifier as `'0'` and retrieve `values['0']`.

### Version 0.6.0

* Array type with single dimension supported.
* `isUtcTimeZone` removed from `DefaultTypeConverter.decodeDateTime()` and related functions.
* `DefaultTypeConverter.decodeDateTime()` converts the DateTime instance to local time by calling `DateTime.toLocal()`.

### Version 0.5.8

* `encodeString()` ignores the `trimNull` argument. Now it always removes the null characters.

### Version 0.5.7

* `Pool.busyConnectionCount` counts only `inUse`.

### Version 0.5.6

* `Connect.runInTransaction` returns the result of the transaction operation.

### Version 0.5.5

* `Pool.debugName` removed.
* Make the pool more likely to *shrink*.

### Version 0.5.4

* `PoolImpl` with two new methods: `pooledConnectionCount` and `busyConnectionCount`

### Version 0.5.3

* Support BigInt.
    * Note: `_PG_NUMERIC` will be converted to `BigInt`, if possible (instead of `String`). Otherwise, it is converted to a `String` instance.

### Version 0.5.2

* Upgrade to Dart 2.5

### Version 0.3.4
 
* Update broken crypto dependency.

### Version 0.3.3

* Fix #73 Properly encode/decode connection uris. Thanks to Martin Manev.
* Permit connection without a password. Thanks to Jirka Daněk.

### Version 0.3.2

* Improve handing of datetimes. Thanks to Joe Conway.
* Remove manually cps transformed async code.
* Fix #58: Establish connections concurrently. Thanks to Tom Yeh.
* Fix #67: URI encode db name so spaces can be used in db name. Thanks to Chad Schwendiman.
* Fix #69: Empty connection pool not establishing connections.

### Version 0.3.1+1

* Expose column information via row.getColumns(). Credit to Jesper Håkansson for this change.

### Version 0.3.0

* A new connection pool with more configuration options.
* Support for json and timestamptz types.
* Utc time zone support.
* User customisable type conversions.
* Improved error handling.
* Connection.onClosed has been removed.
* Some api has been renamed, the original names are still functional but marked as deprecated.
    * import 'package:postgresql/postgresql_pool.dart'  =>  import 'package:postgresql/pool.dart'
    * Pool.destroy() => Pool.stop()
    * The constants were upper case and int type. Now typed and lower camel case to match the style guide.
    * Connection.unhandled => Connection.messages
    * Connection.transactionStatus => Connection.transactionState

  Thanks to Tom Yeh and Petar Sabev for their helpful feedback.
