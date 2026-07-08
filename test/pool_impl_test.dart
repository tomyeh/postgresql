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
         Duration startTimeout = const Duration(seconds: 5),
         Duration? leakDetectionThreshold}) =>
    PoolImpl(
        PoolSettingsImpl(databaseUri: '',
            minConnections: min, maxConnections: max,
            testConnections: testConnections,
            connectionTimeout: connectionTimeout, startTimeout: startTimeout,
            leakDetectionThreshold: leakDetectionThreshold),
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

  test('waiter gets a live connection when its pconn dies during handoff',
      () async {
    //simulates a 57P server-shutdown error landing in the microtask between
    //handoff (reserved, completer fired) and the waiter resuming.
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c1 = await pool.connect(); //saturates the pool (max 1)
    final waiter = pool.connect(); //must queue
    await pumpEventQueue();
    expect(pool.waitQueueLength, 1);

    c1.close(); //hands the freed conn to the waiter (reserved, not yet resumed)
    //destroy it synchronously, before the waiter's await resumes
    pool.connections
        .firstWhere((c) => c.state == PooledConnectionState.reserved)
        .destroy();

    final c2 = await waiter.timeout(const Duration(seconds: 2),
        onTimeout: () => throw StateError('waiter stranded by handoff race'));
    expect(c2.state, ConnectionState.idle,
        reason: 'waiter must receive a live connection, not the destroyed one');
    c2.close();
    await pool.stop();
  });

  test('handed-over conn dies silently (socket error): dropped, not stranded',
      () async {
    //unlike destroy(), a socket error closes the connection WITHOUT
    //notifying the pool — the pconn must not leak a slot in `reserved`
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c1 = await pool.connect();
    final waiter = pool.connect(); //must queue
    await pumpEventQueue();
    c1.close(); //hands the freed conn to the waiter (reserved)
    f.conns.single.destroy(); //the socket dies; the pool is not told

    final c2 = await waiter.timeout(const Duration(seconds: 2),
        onTimeout: () => throw StateError('waiter stranded by dead handoff'));
    expect(c2.state, ConnectionState.idle);
    expect(
        pool.connections
            .where((c) => c.state == PooledConnectionState.reserved),
        isEmpty,
        reason: 'the dead pconn must be reclaimed, not stranded in reserved');
    expect(pool.pooledConnectionCount, 1);
    c2.close();
    await pool.stop();
  });

  test('double destroy() of the same pooled connection replenishes once',
      () async {
    final f = _Factory();
    final pool = _pool(f, min: 2, max: 5);
    await pool.start();
    final pc = pool.connections.first;
    pc.destroy();
    pc.destroy(); //no-op: already destroyed
    await pumpEventQueue();
    expect(f.created, 3, reason: 'only one replacement established');
    expect(pool.pooledConnectionCount, 2);
    await pool.stop();
  });

  test('close() then destroy() is a no-op (already returned to pool)',
      () async {
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c = await pool.connect();
    c.close();
    await pumpEventQueue();
    c.destroy(); //released before: must not destroy nor replenish
    await pumpEventQueue();
    expect(f.created, 1, reason: 'no replacement established');
    expect(f.closedCount, 0, reason: 'the pooled connection stays open');
    expect(pool.pooledConnectionCount, 1);
    await pool.stop();
  });

  test('destroy() then close() has no further effect', () async {
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c = await pool.connect();
    c.destroy();
    c.close(); //released before: no double release
    await pumpEventQueue();
    expect(f.created, 2, reason: 'exactly one replacement');
    expect(f.closedCount, 1);
    expect(pool.pooledConnectionCount, 1);
    await pool.stop();
  });

  test('destroy() after stop() neither throws nor establishes', () async {
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final pc = pool.connections.first;
    await pool.stop();
    expect(f.created, 1);
    pc.destroy(); //already removed by stop(): guard must skip replenish
    await pumpEventQueue();
    expect(f.created, 1, reason: 'no connection established after stop');
    expect(pool.state, PoolState.stopped);
  });

  test('stop() right after destroy() drains cleanly', () async {
    //the replenish kicked off by destroy() races stop(); either way the
    //pool must end stopped with every physical connection closed
    final f = _Factory();
    final pool = _pool(f, min: 1, max: 1);
    await pool.start();
    final c = await pool.connect();
    c.destroy();
    await pool.stop();
    expect(pool.state, PoolState.stopped);
    expect(pool.pooledConnectionCount, 0);
    expect(f.conns.every((c) => c.closed), isTrue,
        reason: 'no physical connection may survive stop()');
  });

  test('heartbeat replenishes to minConnections', () async {
    //a failed borrow test destroys the only conn without replenishing;
    //the heartbeat (1s, via leakDetectionThreshold 3s) must refill to min
    final f = _Factory(queryHangs: true);
    final pool = _pool(f, min: 1, max: 2, testConnections: true,
        connectionTimeout: const Duration(milliseconds: 1),
        leakDetectionThreshold: const Duration(seconds: 3));
    await pool.start();
    await expectLater(pool.connect(), throwsA(anything));
    await pumpEventQueue();
    expect(pool.pooledConnectionCount, 0);
    await Future.delayed(const Duration(milliseconds: 1500));
    expect(pool.pooledConnectionCount, 1, reason: 'heartbeat refilled to min');
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
