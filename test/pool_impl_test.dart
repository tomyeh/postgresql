import 'dart:async';
import 'package:test/test.dart';
import 'package:postgresql2/postgresql.dart';
import 'package:postgresql2/pool.dart';
import 'package:postgresql2/src/pool_impl.dart' show PoolImpl;
import 'package:postgresql2/src/pool_settings_impl.dart' show PoolSettingsImpl;

/// A fake [Connection] that never touches a socket, so the pool's state
/// machine can be exercised deterministically without a database.
class _FakeConn implements Connection {
  _FakeConn(this.id, {this.queryHangs = false});
  final int id;
  final bool queryHangs;
  final _msgs = StreamController<Message>.broadcast();
  bool closed = false;

  @override
  Stream<Message> get messages => _msgs.stream;
  @override
  int? get backendPid => id;
  @override
  ConnectionState get state =>
      closed ? ConnectionState.closed: ConnectionState.idle;
  @override
  TransactionState get transactionState => TransactionState.none;

  @override //hangs → `select true` never resolves, forcing a test timeout
  Stream<Row> query(String sql, [Map? values]) =>
      queryHangs ? StreamController<Row>().stream: Stream<Row>.empty();

  @override
  void close() => _close();
  @override
  void destroy() => _close();
  void _close() {
    if (closed) return;
    closed = true;
    _msgs.close();
  }

  @override
  dynamic noSuchMethod(Invocation i) => super.noSuchMethod(i);
}

/// A [ConnectionFactory] returning [_FakeConn]s, tracking how many were
/// created and (optionally) failing from the [failFrom]-th call onward.
class _Factory {
  _Factory({this.failFrom, this.queryHangs = false});
  final int? failFrom; //1-based call index at/after which connect() fails
  final bool queryHangs;
  int created = 0;
  final conns = <_FakeConn>[];

  int get closedCount => conns.where((c) => c.closed).length;

  Future<Connection> connect(String uri,
      {Duration? connectionTimeout, String? applicationName,
       String? timeZone, TypeConverter? typeConverter, String? debugName}) {
    final n = ++created;
    if (failFrom != null && n >= failFrom!)
      return Future.error(Exception('connect failure #$n'));
    final c = _FakeConn(n, queryHangs: queryHangs);
    conns.add(c);
    return Future.value(c);
  }
}

PoolImpl _pool(_Factory f,
        {int min = 2, int max = 2, bool testConnections = false,
         Duration connectionTimeout = const Duration(seconds: 5),
         Duration startTimeout = const Duration(seconds: 5)}) =>
    PoolImpl(
        PoolSettingsImpl(databaseUri: '',
            minConnections: min, maxConnections: max,
            testConnections: testConnections,
            connectionTimeout: connectionTimeout, startTimeout: startTimeout),
        null, f.connect);

void main() {
  test('start establishes minConnections then stop drains', () async {
    final f = _Factory();
    final pool = _pool(f, min: 3, max: 5);
    await pool.start();
    expect(pool.state, PoolState.running);
    expect(pool.pooledConnectionCount, 3);
    expect(f.created, 3);
    await pool.stop();
    expect(pool.state, PoolState.stopped);
    expect(pool.pooledConnectionCount, 0);
    expect(f.closedCount, 3, reason: 'stop() closes every connection');
  });

  test('failed start ends in startFailed and destroys what it established',
      () async {
    //calls 1,2 succeed; 3,4,5 throw → the 2 successes must be cleaned up
    final f = _Factory(failFrom: 3);
    final pool = _pool(f, min: 5, max: 5);
    await expectLater(pool.start(), throwsA(isA<Exception>()));
    expect(pool.state, PoolState.startFailed);
    expect(pool.pooledConnectionCount, 0,
        reason: 'established connections must not leak on start failure');
    expect(f.closedCount, 2,
        reason: 'the 2 successful sockets were closed');
  });

  test('connect/close returns the connection to the pool (reuse)', () async {
    final f = _Factory();
    final pool = _pool(f, min: 2, max: 2);
    await pool.start();
    final c = await pool.connect();
    expect(pool.busyConnectionCount, 1);
    c.close();
    await pumpEventQueue();
    expect(pool.busyConnectionCount, 0);
    expect(pool.pooledConnectionCount, 2);
    expect(f.created, 2, reason: 'close() reuses, does not create');
    await pool.stop();
  });

  test('destroy() physically closes and replenishes toward minConnections',
      () async {
    final f = _Factory();
    final pool = _pool(f, min: 2, max: 2);
    await pool.start();
    expect(f.created, 2);
    final c = await pool.connect();
    c.destroy();
    await pumpEventQueue();
    expect(pool.pooledConnectionCount, 2, reason: 'pool refilled to min');
    expect(f.created, 3, reason: 'a replacement connection was established');
    expect(f.closedCount, 1, reason: 'the destroyed connection was closed');
    await pool.stop();
  });

  test('destroy() frees capacity and serves a queued waiter', () async {
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c1 = await pool.connect(); //saturates the pool (max 1)
    final waiter = pool.connect(); //must queue
    await pumpEventQueue();
    expect(pool.waitQueueLength, 1);

    c1.destroy(); //frees the slot, re-establishes, serves the waiter
    final c2 = await waiter.timeout(const Duration(seconds: 2),
        onTimeout: () => throw StateError('waiter stranded by destroy()'));
    expect(c2, isNotNull);
    c2.close();
    await pool.stop();
  });

  test('failed connection test destroys the conn (no `testing` zombie)',
      () async {
    //testConnections + a hanging `select true` → the borrow test times out
    //with no budget left; the pconn must be destroyed, not left in `testing`.
    final f = _Factory(queryHangs: true);
    final pool = _pool(f, min: 1, max: 2, testConnections: true,
        connectionTimeout: const Duration(milliseconds: 20));
    await pool.start();
    await expectLater(pool.connect(), throwsA(anything));
    await pumpEventQueue();
    final stuck = pool.connections
        .where((c) => c.state == PooledConnectionState.testing).length;
    expect(stuck, 0, reason: 'a failed-test connection must be reclaimed');
    await pool.stop();
  });
}
