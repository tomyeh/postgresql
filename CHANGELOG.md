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

#### Version 0.3.3

* Fix #73 Properly encode/decode connection uris. Thanks to Martin Manev.
* Permit connection without a password. Thanks to Jirka Daněk.

#### Version 0.3.2

* Improve handing of datetimes. Thanks to Joe Conway.
* Remove manually cps transformed async code.
* Fix #58: Establish connections concurrently. Thanks to Tom Yeh.
* Fix #67: URI encode db name so spaces can be used in db name. Thanks to Chad Schwendiman.
* Fix #69: Empty connection pool not establishing connections.

#### Version 0.3.1+1

* Expose column information via row.getColumns(). Credit to Jesper Håkansson for this change.

#### Version 0.3.0

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
