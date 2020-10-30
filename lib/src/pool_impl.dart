library postgresql.pool.impl;

import 'dart:async';
import 'dart:collection';
import 'dart:io';
import 'dart:math' as math;
import 'package:postgresql2/constants.dart';
import 'package:postgresql2/postgresql.dart' as pg;
import 'package:postgresql2/src/postgresql_impl/postgresql_impl.dart' as pgi;
import 'package:postgresql2/pool.dart';


// I like my enums short and sweet, not long and typey.
const connecting = PooledConnectionState.connecting,
  available = PooledConnectionState.available,
  reserved = PooledConnectionState.reserved,
  testing = PooledConnectionState.testing,
  inUse = PooledConnectionState.inUse,
  connClosed = PooledConnectionState.closed;

typedef Future<pg.Connection> ConnectionFactory(
    String uri,
    {Duration connectionTimeout,
     String applicationName,
     String timeZone,
     pg.TypeConverter typeConverter,
     String getDebugName(),
     Future<Socket> mockSocketConnect(String host, int port)});

class ConnectionDecorator implements pg.Connection {

  ConnectionDecorator(this._pool, PooledConnectionImpl pconn, this._conn)
      : _pconn = pconn;

  _error(fnName) => new pg.PostgresqlException(
      '$fnName() called on closed connection.', _pconn.name);

  bool _isReleased = false;
  final pg.Connection _conn;
  final PoolImpl _pool;
  final PooledConnectionImpl _pconn;

  void close() {
    if (!_isReleased) _pool._releaseConnection(_pconn);
    _isReleased = true;
  }

  Stream<pg.Row> query(String sql, [values]) => _isReleased
      ? throw _error('query')
      : _conn.query(sql, values);

  Future<int> execute(String sql, [values]) => _isReleased
      ? throw _error('execute')
      : _conn.execute(sql, values);

  Future<T> runInTransaction<T>(Future<T> operation(),
                          [pg.Isolation isolation = readCommitted])
    => _isReleased
        ? throw throw _error('runInTransaction')
        : _conn.runInTransaction(operation, isolation);

  pg.ConnectionState get state => _isReleased ? closed : _conn.state;

  pg.TransactionState get transactionState => _isReleased
      ? unknown
      : _conn.transactionState;

  Stream<pg.Message> get messages => _isReleased
    ? new Stream.fromIterable([])
    : _conn.messages;

  Map<String,String> get parameters => _isReleased ? {} : _conn.parameters;

  int get backendPid => _conn.backendPid;

  @override
  String toString() => "$_pconn";
}


class PooledConnectionImpl implements PooledConnection {

  PooledConnectionImpl(this._pool);

  final PoolImpl _pool;
  pg.Connection _connection;
  PooledConnectionState _state;
  DateTime _established;
  DateTime _obtained;
  DateTime _released;
  int _useId;
  bool _isLeaked = false;
  StackTrace _stackTrace;
  
  final Duration _random = new Duration(seconds: new math.Random().nextInt(20));
  
  PooledConnectionState get state => _state;

  DateTime get established => _established;

  DateTime get obtained => _obtained;

  DateTime get released => _released;
  
  int get backendPid => _connection == null ? null : _connection.backendPid;

  int get useId => _useId;
  
  bool get isLeaked => _isLeaked;

  StackTrace get stackTrace => _stackTrace;
  
  pg.ConnectionState get connectionState
    => _connection == null ? null : _connection.state;
  
  String get name => '${_pool.settings.poolName}:$backendPid'
      + (_useId == null ? '' : ':$_useId');

  String toString() => '$name:$_state:$connectionState';
}

class PoolImpl implements Pool {

  PoolImpl(PoolSettings settings,
        this._typeConverter,
       [this._connectionFactory = pgi.ConnectionImpl.connect])
      : settings = settings == null ? new PoolSettings() : settings;
      
  PoolState _state = initial;
  PoolState get state => _state;

  final PoolSettings settings;
  final pg.TypeConverter _typeConverter;
  final ConnectionFactory _connectionFactory;
  
  //TODO Consider using a list instead. removeAt(0); instead of removeFirst().
  // Since the list will be so small there is not performance benefit using a
  // queue.
  final _waitQueue = new Queue<Completer<PooledConnectionImpl>>();

  Timer _heartbeatTimer;
  Duration _heartbeatDuration;
  Future _stopFuture;
  
  final _messages = new StreamController<pg.Message>.broadcast();
  final _connections = new List<PooledConnectionImpl>();
  
  List<PooledConnectionImpl> _connectionsView; 
  
  List<PooledConnectionImpl> get connections {
    if (_connectionsView == null)
      _connectionsView = new UnmodifiableListView(_connections);
    return _connectionsView;
  }

  @override
  int get pooledConnectionCount => _connections.length;
  @override
  int get busyConnectionCount {
    int count = 0;
    for (final conn in _connections)
      if (conn._state == inUse)
        ++count;
    return count;
  }

  int get waitQueueLength => _waitQueue.length;
  
  Stream<pg.Message> get messages => _messages.stream;

  Future start() async {
    //TODO consider allowing moving from state stopped to starting.
    //Need to carefully clear out all state.
    if (_state != initial)
      throw new pg.PostgresqlException(
          'Cannot start connection pool while in state: $_state.', null);

    var stopwatch = new Stopwatch()..start();

    var onTimeout = () {
      _state = startFailed;
      throw new pg.PostgresqlException(
        'Connection pool start timed out with: '
          '${settings.startTimeout}).', null);
    };

    _state = starting;

    // Start connections in parallel.
    var futures = new Iterable.generate(settings.minConnections,
        (i) => _establishConnection());
      //don't call ...Safely so exception will be sent to caller

    await Future.wait(futures)
      .timeout(settings.startTimeout, onTimeout: onTimeout);

    // If something bad happened and there are not enough connecitons.
    while (_connections.length < settings.minConnections) {
      await _establishConnection()
        .timeout(settings.startTimeout - stopwatch.elapsed, onTimeout: onTimeout);
    }

    _state = running;

    //heartbeat is used to detect leak and destroy idle connection
    final leakMilliseconds = settings.leakDetectionThreshold != null ?
        math.max(1000, settings.leakDetectionThreshold.inMilliseconds ~/ 3):
        500*60*1000; //bigger than possible [idleTimeout]
    _heartbeatDuration = new Duration(milliseconds:
        math.min(leakMilliseconds,
          math.max(60000, settings.idleTimeout.inMilliseconds ~/ 3)));
    _heartbeat(); //start heartbeat
  }
  
  Future _establishConnection() async {
    // Do nothing if called while shutting down.
    if (!(_state == running || _state == PoolState.starting))
      return;
    
    // This shouldn't be able to happen - but is here for robustness.
    if (_connections.length >= settings.maxConnections)
      return;
    
    var pconn = new PooledConnectionImpl(this);
    pconn._state = connecting;
    _connections.add(pconn);

    try {
      var conn = await _connectionFactory(
        settings.databaseUri,
        connectionTimeout: settings.establishTimeout,
        applicationName: settings.applicationName,
        timeZone: settings.timeZone,
        typeConverter: _typeConverter,
        getDebugName: () => pconn.name);

      // Pass this connection's messages through to the pool messages stream.
      conn.messages.listen((msg) => _messages.add(msg),
          onError: (msg) => _messages.addError(msg));

      pconn._connection = conn;
      pconn._established = new DateTime.now(); 
      pconn._state = available;
    } catch (_) {
      _connections.remove(pconn); //clean zombies
      rethrow;
    }
  }

  //A safe version that catches the exception.
  Future _establishConnectionSafely()
  => _establishConnection()
    .catchError((ex) {
      _messages.add(new pg.ClientMessage(
          severity: 'WARNING',
          message: "Failed to establish connection",
          exception: ex));
      return ex; //so caller can handle errors
    });
  
  void _heartbeat() {
    if (_state != running) return;

    try {
      if (settings.leakDetectionThreshold != null)
        _forEachConnection(_checkIfLeaked);

      for (int i = _connections.length;
          _connections.length > settings.minConnections
          && --i >= 0;) //reverse since it might be removed
        _checkIdleTimeout(_connections[i], i);

      // This shouldn't be necessary, but should help fault tolerance. 
      _processWaitQueue();

      _checkIfAllConnectionsLeaked();

    } finally {
      _heartbeatTimer = new Timer(_heartbeatDuration, _heartbeat);
    }
  }

  void _checkIdleTimeout(PooledConnectionImpl pconn, int i) {
    if (pconn._state == available
    && _isExpired(pconn._released ?? pconn._established, settings.idleTimeout)) {
      _destroyConnection(pconn, i);
    }
  }
  
  void _checkIfLeaked(PooledConnectionImpl pconn, int i) {
    if (!pconn._isLeaked
        && pconn._state != available
        && pconn._obtained != null
        && _isExpired(pconn._obtained, settings.leakDetectionThreshold)) {
      pconn._isLeaked = true;
      _messages.add(new pg.ClientMessage(
          severity: 'WARNING',
          connectionName: pconn.name,
          message: 'Leak detected. '
            'state: ${pconn._connection.state} '
            'transactionState: ${pconn._connection.transactionState} '
            'stacktrace: ${pconn._stackTrace}'));
    }
  }
  
  int get _leakedConnections =>
    _connections.where((c) => c._isLeaked).length;
  
  /// If all connections are in leaked state, then destroy them all, and
  /// restart the minimum required number of connections.
  void _checkIfAllConnectionsLeaked() {
    if (settings.restartIfAllConnectionsLeaked
        && _leakedConnections >= settings.maxConnections) {

      _messages.add(new pg.ClientMessage(
          severity: 'WARNING',
          message: '${settings.poolName} is full of leaked connections. '
            'These will be closed and new connections started.'));
      
      // Forcefully close leaked connections.
      _forEachConnection(_destroyConnection);
      
      // Start new connections in parallel.
      for (int i = 0; i < settings.minConnections; i++) {
        _establishConnectionSafely();
      }
    }
  }
  
  // Used to generate unique ids (well... unique for this isolate at least).
  static int _sequence = 1;

  Future<pg.Connection> connect() async {
    if (_state != running)
      throw new pg.PostgresqlException(
        'Connect called while pool is not running.', null,
        exception: pePoolStopped);
    
    StackTrace stackTrace = null;
    if (settings.leakDetectionThreshold != null) {
      // Store the current stack trace for connection leak debugging.
      stackTrace = StackTrace.current;
    }

    var pconn = await _connect(settings.connectionTimeout);

    assert((settings.testConnections && pconn._state == testing)
        || (!settings.testConnections && pconn._state == reserved));
    assert(pconn._connection.state == idle);
    assert(pconn._connection.transactionState == none);    
    
    pconn.._state = inUse
      .._obtained = new DateTime.now()
      .._useId = _sequence++
      .._stackTrace = stackTrace;

    return new ConnectionDecorator(this, pconn, pconn._connection);
  }

  Future<PooledConnectionImpl> _connect(Duration timeout) async {

    if (state == stopping || state == stopped)
      throw new pg.PostgresqlException(
          'Connect failed as pool is stopping.', null, exception: pePoolStopped);
    
    var stopwatch = new Stopwatch()..start();

    var pconn = _getNextAvailable();
    
    timeoutException() => new pg.PostgresqlException(
      'Obtaining connection from pool exceeded timeout: '
        '${settings.connectionTimeout}.\nAlive connections: ${_connections.length}', 
            pconn?.name, exception: peConnectionTimeout);
   
    // If there are currently no available connections then
    // add the current connection request at the end of the
    // wait queue.
    if (pconn == null) {
      var c = new Completer<PooledConnectionImpl>();
      _waitQueue.add(c);
      try {
        _processWaitQueue();
        pconn = await c.future.timeout(timeout, onTimeout: () => throw timeoutException());
      } finally {
        _waitQueue.remove(c);
      }
      assert(pconn.state == reserved);
    }
    
    if (!settings.testConnections) {
      pconn._state = reserved;
      return pconn;
    }
    
    pconn._state = testing;
        
    if (await _testConnection(pconn, timeout - stopwatch.elapsed, () => throw timeoutException()))
      return pconn;
    
    if (timeout > stopwatch.elapsed) {
      throw timeoutException();
    } else {
      _destroyConnection(pconn);
      // Get another connection out of the pool and test again.
      return _connect(timeout - stopwatch.elapsed);
    }
  }

  /// Next available connection.
  /// Starts from the same direction, so it is more likely to reduce the pool
  /// (i.e., [idleTimeout] likely expired)
  PooledConnectionImpl _getNextAvailable() {
    for (final pconn in _connections)
      if (pconn._state == available)
        return pconn;
    return null;
  }

  /// If connections are available, return them to waiting clients.
  void _processWaitQueue([List previousResults]) {
    
    if (_state != running) return;
    
    if (_waitQueue.isEmpty) return;

    // Scan from 0 (same as [_getNextAvailable])
    for (int i = 0; _waitQueue.isNotEmpty && i < _connections.length; ++i) {
      var pconn = _connections[i];
      if (pconn._state == available) {
        var completer = _waitQueue.removeFirst();
        pconn._state = reserved;
        completer.complete(pconn);
      }
    }

    //Handle the error(s) found in the previous invocation if any
    //Purpose: make the caller of [connect] to end as soon as possible.
    //Otherwise, it will wait until timeout
    if (_waitQueue.isNotEmpty && previousResults != null) {
      for (final r in previousResults)
        if (r is SocketException) { //unable to connect DB server
          final ex = new pg.PostgresqlException(
              'Failed to establish connection', 
              null, exception: peConnectionFailed);
          while (_waitQueue.isNotEmpty)
            _waitQueue.removeFirst().completeError(ex);
        }
    }

    // If required start more connection.
    if (!_establishing) { //once at a time
      final int count = math.min(_waitQueue.length,
          settings.maxConnections - _connections.length);
      if (count > 0) {
        _establishing = true;
        List results;
        new Future.sync(() {
          final List<Future> ops = new List(count);
          for (int i = 0; i < count; i++) {
            ops[i] = _establishConnectionSafely();
          }
          return Future.wait(ops);
        })
        .then((_) {
          results = _;
        })
        .whenComplete(() {
          _establishing = false;

          _processWaitQueue(results); //do again; there might be more requests
        });
      }
    }
  }
  bool _establishing = false;

  /// Perfom a query to check the state of the connection.
  Future<bool> _testConnection(
      PooledConnectionImpl pconn,
      Duration timeout, 
      Function onTimeout) async {
    bool ok;
    try {
      var row = await pconn._connection.query('select true')
                         .single.timeout(timeout);
      ok = row[0];
    } catch (ex) { //TODO Do I really want to log warnings when the connection timeout fails.
      ok = false;
      // Don't log connection test failures during shutdown.
      if (state != stopping && state != stopped) {
        var msg = ex is TimeoutException
              ? 'Connection test timed out.'
              : 'Connection test failed.';
        _messages.add(new pg.ClientMessage(
            severity: 'WARNING',
            connectionName: pconn.name,
            message: msg,
            exception: ex));
      }
    }
    return ok;
  }

  void _releaseConnection(PooledConnectionImpl pconn) {
    if (state == stopping || state == stopped) {
      _destroyConnection(pconn);
      return;
    }
    
    assert(pconn._pool == this);
    assert(_connections.contains(pconn));
    assert(pconn.state == inUse);
    
    pg.Connection conn = pconn._connection;
    
    // If connection still in transaction or busy with query then destroy.
    // Note this means connections which are returned with an un-committed 
    // transaction, the entire connection will be destroyed and re-established.
    // While it would be possible to write code which would send a rollback 
    // command, this is simpler and probably nearly as fast (not that this
    // is likely to become a bottleneck anyway).
    if (conn.state != idle || conn.transactionState != none) {
        _messages.add(new pg.ClientMessage(
            severity: 'WARNING',
            connectionName: pconn.name,
            message: 'Connection returned in bad state. Removing from pool. '
              'state: ${conn.state} '
              'transactionState: ${conn.transactionState}.'));

        _destroyConnection(pconn);
        _establishConnectionSafely();

    // If connection older than lifetime setting then destroy.
    // A random number of seconds 0-20 is added, so that all connections don't
    // expire at exactly the same moment.
    } else if (settings.maxLifetime != null
        && _isExpired(pconn._established, settings.maxLifetime + pconn._random)) {
      _destroyConnection(pconn);
      _establishConnectionSafely();

    } else {
      pconn._released = new DateTime.now();
      pconn._state = available;
      _processWaitQueue();
    }
  }
  
  bool _isExpired(DateTime time, Duration timeout) 
    => new DateTime.now().difference(time) > timeout;
  
  void _destroyConnection(PooledConnectionImpl pconn, [int i]) {
    if (pconn._connection != null) pconn._connection.close();
    pconn._state = connClosed;

    //revere order since we clean up from the end
    if (i != null && pconn == _connections[i]) {
      _connections.removeAt(i);
    } else {
      for (int i = _connections.length; --i >= 0;)
        if (pconn == _connections[i]) {
          _connections.removeAt(i);
          break;
        }
    }
  }
  
  Future stop() {
    if (state == stopped || state == initial) return null;
      
    if (_stopFuture == null)
      _stopFuture = _stop();
    else
      assert(state == stopping);
      
    return _stopFuture;
  }
  
  Future _stop() async {
    _state = stopping;

    if (_heartbeatTimer != null) _heartbeatTimer.cancel();
  
    // Send error messages to connections in wait queue.
    _waitQueue.forEach((completer) =>
      completer.completeError(new pg.PostgresqlException(
          'Connection pool is stopping.', null,
          exception: pePoolStopped)));
    _waitQueue.clear();
    
    
    // Close connections as they are returned to the pool.
    // If stop timeout is reached then close connections even if still in use.

    var stopwatch = new Stopwatch()..start();
    while (_connections.isNotEmpty) {
      _forEachConnection((pconn, i) {
        if (pconn._state == available)
          _destroyConnection(pconn, i);
      });

      await new Future.delayed(new Duration(milliseconds: 100), () => null);

      if (stopwatch.elapsed > settings.stopTimeout ) {
        _messages.add(new pg.ClientMessage(
            severity: 'WARNING',
            message: 'Exceeded timeout while stopping pool, '
              'closing in use connections.'));        
        // _destroyConnection modifies this list, so need to make a copy.
        _forEachConnection(_destroyConnection);
      }
    }
    _state = stopped;
  }

  void _forEachConnection(f(PooledConnectionImpl pconn, int i)) {
    for (int i = _connections.length; --i >= 0;) //reverse since it might be removed
      f(_connections[i], i);
  }
}
