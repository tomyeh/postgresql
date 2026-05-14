part of postgresql.impl;

///A owner of [Connection].
abstract class ConnectionOwner {
  /// Destroys the connection.
  /// It is called if the connection is no longer available.
  /// For example, server restarts or crashes.
  void destroy();
}

class ConnectionImpl implements Connection {

  ConnectionImpl._private(
      this._socket,
      Settings settings,
      this._applicationName,
      this._timeZone,
      TypeConverter? typeConverter,
      String? debugName)
    : _userName = settings.user,
      _password = settings.password,
      _databaseName = settings.database,
      _typeConverter = typeConverter ?? TypeConverter(),
      _debugName = debugName ?? 'pg',
      _buffer = Buffer((msg) => PostgresqlException(msg, debugName ?? 'pg')),
      _saslAuthenticator = _SaslAuthenticator(ScramAuthenticator(
        'SCRAM-SHA-256', // Optionally choose hash method from a list provided by the server
        sha256,
        UsernamePasswordCredential(username: settings.user, password: settings.password)));

  @override
  ConnectionState get state => _state;
  ConnectionState _state = notConnected;

  TransactionState _transactionState = unknown;
  @override
  TransactionState get transactionState => _transactionState;

  final String _databaseName;
  final String _userName;
  final String _password;
  final String? _applicationName;
  final String? _timeZone;
  final TypeConverter _typeConverter;
  /// The owner of the connection, or null if not available.
  ConnectionOwner? owner;
  final Socket _socket;
  final Buffer _buffer;
  bool _hasConnected = false;
  final _connected = Completer<ConnectionImpl>();
  final Queue<_Query> _sendQueryQueue = Queue<_Query>();
  final _SaslAuthenticator _saslAuthenticator;
  _Query? _query;
  int? _msgType;
  int? _msgLength;
  //int _secretKey;
  int _transactionLevel = 0;

  int? _backendPid;
  final String _debugName;

  @override
  int? get backendPid => _backendPid;

  String get debugName => _debugName;

  @override
  String toString() => '$debugName:$_backendPid';

  final _parameters = Map<String, String>();

  Map<String,String>? _parametersView;

  @override
  Map<String,String> get parameters
  => _parametersView ??
      (_parametersView = UnmodifiableMapView(_parameters));

  @override
  Stream<Message> get messages => _messages.stream;

  final _messages = StreamController<Message>.broadcast();

  static Future<ConnectionImpl> connect(
      String uri,
      {Duration? connectionTimeout,
       String? applicationName,
       String? timeZone,
       TypeConverter? typeConverter,
       String? debugName,
       Future<Socket> mockSocketConnect(String host, int port)?}) async {

    var settings = Settings.fromUri(uri);

    //FIXME Currently this timeout doesn't cancel the socket connection
    // process.
    // There is a bug open about adding a real socket connect timeout
    // parameter to Socket.connect() if this happens then start using it.
    // http://code.google.com/p/dart/issues/detail?id=19120
    connectionTimeout ??= const Duration(seconds: 180);

    var onTimeout = () => throw PostgresqlException(
        'Postgresql connection timed out. Timeout: $connectionTimeout.',
        debugName ?? 'pg', exception: peConnectionTimeout);

    var connectFunc = mockSocketConnect == null
        ? Socket.connect
        : mockSocketConnect;

    Future<Socket> future = connectFunc(settings.host, settings.port)
        .timeout(connectionTimeout, onTimeout: onTimeout);

    if (settings.requireSsl) future = _connectSsl(future);

    final socket = await future.timeout(connectionTimeout, onTimeout: onTimeout);

    var conn = ConnectionImpl._private(socket, settings,
        applicationName, timeZone, typeConverter, debugName);

    late final StreamSubscription<Uint8List> sub;
    sub = socket.listen(conn._readData,
        onError: (ex) {
          conn._handleSocketError(ex);
          InvokeUtil.invokeSafely(sub.cancel);
        },
        onDone: conn._handleSocketClosed);

    conn
      .._state = socketConnected
      .._sendStartupMessage();
    return conn._connected.future.timeout(connectionTimeout, onTimeout: () {
      conn._destroy();
      throw PostgresqlException(
          'Postgresql handshake timed out. Timeout: $connectionTimeout.',
          conn._debugName, exception: peConnectionTimeout);
    });
  }

  static String _md5s(String s) {
    var digest = md5.convert(s.codeUnits.toList());
    return hex.encode(digest.bytes);
  }

  //TODO yuck - this needs a rewrite.
  static Future<SecureSocket> _connectSsl(Future<Socket> future) {

    var completer = Completer<SecureSocket>();

    future.then((socket) {

      socket.listen((data) {
        if (data[0] != _S) {
          completer.completeError(
              PostgresqlException(
                  'This postgresql server is not configured to support SSL '
                  'connections.', null, //FIXME ideally pass the connection pool name through to this exception.
                  exception: peConnectionFailed));
          InvokeUtil.invokeSafely(socket.destroy);
        } else {
          // TODO add option to only allow valid certs.
          // Note libpq also defaults to ignoring bad certificates, so this is
          // expected behaviour.
          // TODO consider adding a warning if certificate is invalid so that it
          // is at least logged.
          SecureSocket.secure(socket, onBadCertificate: (cert) => true)
            .then(completer.complete)
            .catchError((ex, st) {
              if (!completer.isCompleted) completer.completeError(ex, st);
              InvokeUtil.invokeSafely(socket.destroy);
            });
        }
      },
      onError: (ex, st) {
        if (!completer.isCompleted) completer.completeError(ex, st);
      },
      cancelOnError: true);

      // Write header, and SSL magic number.
      socket.add(const [0, 0, 0, 8, 4, 210, 22, 47]);
    })
    .catchError((ex, st) {
      completer.completeError(ex, st);
    });

    return completer.future;
  }

  void _sendStartupMessage() {
    if (_state != socketConnected)
      throw PostgresqlException(
          'Invalid state during startup.', _debugName,
          exception: peConnectionFailed);

    var msg = MessageBuffer();
    msg.addInt32(0); // Length padding.
    msg.addInt32(_PROTOCOL_VERSION);
    msg.addUtf8String('user');
    msg.addUtf8String(_userName);
    msg.addUtf8String('database');
    msg.addUtf8String(_databaseName);
    msg.addUtf8String('client_encoding');
    msg.addUtf8String('UTF8');
    final tz = _timeZone;
    if (tz != null) {
      msg.addUtf8String('TimeZone');
      msg.addUtf8String(tz);
    }
    final app = _applicationName;
    if (app != null) {
      msg.addUtf8String('application_name');
      msg.addUtf8String(app);
    }
    msg.addByte(0);
    msg.setLength(startup: true);

    _socket.add(msg.buffer);

    _state = authenticating;
  }

  void _readAuthenticationRequest(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    if (_state != authenticating)
      throw PostgresqlException(
          'Invalid connection state while authenticating.', _debugName,
          exception: peConnectionFailed);

    int authType = _buffer.readInt32();

    if (authType == _AUTH_TYPE_OK) {
      _state = authenticated;
      return;
    }

    // Only MD5 authentication is supported.
    if (!const {_AUTH_TYPE_MD5, _AUTH_TYPE_SASL, 
        _AUTH_TYPE_SASL_CONTINUE, _AUTH_TYPE_SASL_FINAL}.contains(authType)) {
      throw PostgresqlException('Unsupported or unknown authentication '
          'type: ${_authTypeAsString(authType)}, only MD5 and scram-sha-256 authentication is '
          'supported.', _debugName,
          exception: peConnectionFailed);
    }
    switch(authType) {
    case _AUTH_TYPE_MD5:
      var bytes = _buffer.readBytes(4);
      var salt = String.fromCharCodes(bytes);
      var md5 = 'md5' + _md5s(_md5s(_password + _userName) + salt);
      // Build message.
      var msg = MessageBuffer();
      msg.addByte(_MSG_PASSWORD);
      msg.addInt32(0);
      msg.addUtf8String(md5);
      msg.setLength();

      _socket.add(msg.buffer);
      break;
    case _AUTH_TYPE_SASL:
      _saslAuthenticator.sasl(_socket, _buffer, 
        Uint8List.fromList(_buffer.readBytes(length - 4)));
      break;
    case _AUTH_TYPE_SASL_CONTINUE:
      _saslAuthenticator.saslContinue(_socket, _buffer, 
        Uint8List.fromList(_buffer.readBytes(length - 4)));
      break;
    case _AUTH_TYPE_SASL_FINAL:
      _saslAuthenticator.saslFinal(_socket, _buffer, 
        Uint8List.fromList(_buffer.readBytes(length - 4)));
      break;
    }
  }

  void _readReadyForQuery(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    int c = _buffer.readByte();

    if (c == _I || c == _T || c == _E) {

      if (c == _I)
        _transactionState = none;
      else if (c == _T)
        _transactionState = begun;
      else if (c == _E)
        _transactionState = error;

      var was = _state;

      _state = idle;

      _query?.close();
      _query = null;

      if (was == authenticated) {
        _hasConnected = true;
        _connected.complete(this);
      }

      Timer.run(_processSendQueryQueue);

    } else {
      _destroy();
      throw PostgresqlException('Unknown ReadyForQuery transaction status: '
          '${_itoa(c)}.', _debugName);
    }
  }

  void _handleSocketError(error, {bool closed = false}) {

    if (_state == ConnectionState.closed) {
      _messages.add(ClientMessageImpl(
          isError: false,
          severity: 'WARNING',
          message: 'Socket error after socket closed.',
          connectionName: _debugName,
          exception: error));
      _destroy();
      return;
    }

    _destroy();

    var msg = closed ? 'Socket closed unexpectedly.' : 'Socket error.';

    if (!_hasConnected) {
      _connected.completeError(PostgresqlException(msg, debugName,
          exception: error));
    } else {
      final ex = PostgresqlException(msg, debugName, exception: error);
      if (!_abortPendingQueries(ex))
        _messages.add(ClientMessage(
            isError: true, connectionName: debugName, severity: 'ERROR',
            message: msg, exception: error));
    }
  }

  /// Aborts the active query and every queued query with [ex]; closes their
  /// streams so listeners don't hang. Returns true if anything was aborted.
  bool _abortPendingQueries(PostgresqlException ex) {
    bool any = false;
    if ( _query case final query?) {
      if (!query._controller.isClosed) {
        query.addError(ex);
        query.close();
      }
      _query = null;
      any = true;
    }
    while (_sendQueryQueue.isNotEmpty) {
      final q = _sendQueryQueue.removeFirst();
      if (!q._controller.isClosed) {
        q.addError(ex);
        q.close();
      }
      any = true;
    }
    return any;
  }

  void _handleSocketClosed() {
    if (_state != closed) {
      _handleSocketError(null, closed: true);
    }
  }

  void _readData(List<int> data) {

    try {

      if (_state == closed)
        return;

      _buffer.append(data);

      // Handle resuming after storing message type and length.
      final msgType = _msgType;
      if (msgType != null) {
        final msgLength = _msgLength!;
        if (msgLength > _buffer.bytesAvailable)
            return; // Wait for entire message to be in buffer.

        _readMessage(msgType, msgLength);

        _msgType = null;
        _msgLength = null;
      }

      // Main message loop.
      while (_state != closed) {

        if (_buffer.bytesAvailable < 5)
          return; // Wait for more data.

        // Message length is the message length excluding the message type code, but
        // including the 4 bytes for the length fields. Only the length of the body
        // is passed to each of the message handlers.
        int msgType = _buffer.readByte();
        int length = _buffer.readInt32() - 4;

        if (!_checkMessageLength(msgType, length + 4)) {
          throw PostgresqlException('Lost message sync.', debugName);
        }

        if (length > _buffer.bytesAvailable) {
          // Wait for entire message to be in buffer.
          // Store type, and length for when more data becomes available.
          _msgType =  msgType;
          _msgLength = length;
          return;
        }

        _readMessage(msgType, length);
      }

    } catch (_) {
      _destroy();
      rethrow;
    }
  }

  bool _checkMessageLength(int msgType, int msgLength) {

    if (_state == authenticating) {
      if (msgLength < 8) return false;
      if (msgType == _MSG_AUTH_REQUEST && msgLength > 2000) return false;
      if (msgType == _MSG_ERROR_RESPONSE && msgLength > 30000) return false;
    } else {
      if (msgLength < 4) return false;

      // These are the only messages from the server which may exceed 30,000
      // bytes.
      if (msgLength > 30000 && (msgType != _MSG_NOTICE_RESPONSE
          && msgType != _MSG_ERROR_RESPONSE
          && msgType != _MSG_COPY_DATA
          && msgType != _MSG_ROW_DESCRIPTION
          && msgType != _MSG_DATA_ROW
          && msgType != _MSG_FUNCTION_CALL_RESPONSE
          && msgType != _MSG_NOTIFICATION_RESPONSE)) {
        return false;
      }
    }
    return true;
  }

  void _readMessage(int msgType, int length) {

    int pos = _buffer.bytesRead;

    switch (msgType) {

      case _MSG_AUTH_REQUEST:     _readAuthenticationRequest(msgType, length); break;
      case _MSG_READY_FOR_QUERY:  _readReadyForQuery(msgType, length); break;

      case _MSG_ERROR_RESPONSE:
      case _MSG_NOTICE_RESPONSE:
          _readErrorOrNoticeResponse(msgType, length); break;

      case _MSG_BACKEND_KEY_DATA: _readBackendKeyData(msgType, length); break;
      case _MSG_PARAMETER_STATUS: _readParameterStatus(msgType, length); break;

      case _MSG_ROW_DESCRIPTION:  _readRowDescription(msgType, length); break;
      case _MSG_DATA_ROW:         _readDataRow(msgType, length); break;
      case _MSG_EMPTY_QUERY_REPONSE: assert(length == 0); break;
      case _MSG_COMMAND_COMPLETE: _readCommandComplete(msgType, length); break;

      default:
        throw PostgresqlException('Unknown, or unimplemented message: '
            '${utf8.decode([msgType])}.', debugName);
    }

    if (pos + length != _buffer.bytesRead)
      throw PostgresqlException('Lost message sync.', debugName);
  }

  void _readErrorOrNoticeResponse(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);

    var map = Map<String, String>();
    int errorCode = _buffer.readByte();
    while (errorCode != 0) {
      var msg = _buffer.readUtf8String(length); //TODO check length remaining.
      map[String.fromCharCode(errorCode)] = msg;
      errorCode = _buffer.readByte();
    }

    var msg = ServerMessageImpl(
        msgType == _MSG_ERROR_RESPONSE, map, debugName);

    var ex = PostgresqlException(msg.message, debugName,
        serverMessage: msg, exception: msg.code);

    if (msgType == _MSG_ERROR_RESPONSE) {
      if (!_hasConnected) {
          _state = closed;
          _socket.destroy();
          _connected.completeError(ex);
      } else {
        final query = _query;
        if (query != null) {
          query.addError(ex);
        } else {
          _messages.add(msg);
        }

        if (msg.code?.startsWith('57P') ?? false) { //PG stop/restart
          final ow = owner;
          if (ow != null) ow.destroy();
          else {
            _state = closed;
            _abortPendingQueries(ex);
            _socket.destroy();
          }
        }
      }
    } else {
      _messages.add(msg);
    }
  }

  void _readBackendKeyData(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);
    _backendPid = _buffer.readInt32();
    /*_secretKey =*/ _buffer.readInt32();
  }

  void _readParameterStatus(int msgType, int length) {
    assert(_buffer.bytesAvailable >= length);
    var name = _buffer.readUtf8String(10000);
    var value = _buffer.readUtf8String(10000);

    warn(msg) {
      _messages.add(ClientMessageImpl(
        severity: 'WARNING',
        message: msg,
        connectionName: debugName));
    }

    _parameters[name] = value;

    // Cache this value so that it doesn't need to be looked up from the map.
    //if (name == 'TimeZone') _isUtcTimeZone = value == 'UTC';

    if (name == 'client_encoding' && value != 'UTF8') {
      warn('client_encoding parameter must remain as UTF8 for correct string '
           'handling. client_encoding is: "$value".');
    }
  }

  @override
  Stream<Row> query(String sql, [Map? values]) {
    try {
      if (values != null)
        sql = substitute(sql, values, _typeConverter.encode);

      return _enqueueQuery(sql).stream;
    } catch (ex, st) {
      return Stream.error(ex, st);
    }
  }

  @override
  Stream<Row> queryByList(String sql, List? values) {
    try {
      if (values != null)
        sql = substituteByList(sql, values, _typeConverter.encode);

      return _enqueueQuery(sql).stream;
    } catch (ex, st) {
      return Stream.error(ex, st);
    }
  }

  @override
  Future<int> execute(String sql, [Map? values]) async {
    if (values != null)
      sql = substitute(sql, values, _typeConverter.encode);

    var query = _enqueueQuery(sql);
    await query.stream.isEmpty;
    return query._rowsAffected ?? 0;
  }

  @override
  Future<int> executeByList(String sql, List? values) async {
    if (values != null)
      sql = substituteByList(sql, values, _typeConverter.encode);

    var query = _enqueueQuery(sql);
    await query.stream.isEmpty;
    return query._rowsAffected ?? 0;
  }

  @override
  Future<T> runInTransaction<T>(Future<T> operation(), [Isolation isolation = Isolation.readCommitted]) async {
    String begin;
    String commit;
    String rollback;
    if (_transactionLevel > 0) {
      final name = 'sp$_transactionLevel';
      begin = 'savepoint $name';
      commit = 'release savepoint $name';
      rollback = 'rollback to savepoint $name';
    } else {
      if (isolation == Isolation.repeatableRead) {
        begin = 'begin; set transaction isolation level repeatable read;';
      } else if (isolation == Isolation.serializable) {
        begin = 'begin; set transaction isolation level serializable;';
      } else {
        begin = 'begin';
      }
      commit = 'commit';
      rollback = 'rollback';
    }
    try {
      ++_transactionLevel;
      await execute(begin);
      final result = await operation();
      await execute(commit);
      return result;
    } catch (_) {
      try {
        await execute(rollback);
      } catch (_) {
      }
      rethrow;
    } finally {
      assert(_transactionLevel > 0);
      --_transactionLevel;
    }
  }

  _Query _enqueueQuery(String sql) {

    if (sql == '')
      throw PostgresqlException(
          'SQL query is null or empty.', debugName);

    if (sql.contains('\u0000'))
      throw PostgresqlException(
          'Sql query contains a null character.', debugName);

    if (_state == closed)
      throw PostgresqlException(
          'Connection is closed, cannot execute query.', debugName,
          exception: peConnectionClosed);

    var query = _Query(sql);
    _sendQueryQueue.addLast(query);

    Timer.run(_processSendQueryQueue);

    return query;
  }

  void _processSendQueryQueue() {

    if (_sendQueryQueue.isEmpty)
      return;

    if (_query != null)
      return;

    if (_state == closed)
      return;

    assert(_state == idle);

    final query = _query = _sendQueryQueue.removeFirst();

    var msg = MessageBuffer();
    msg.addByte(_MSG_QUERY);
    msg.addInt32(0); // Length padding.
    msg.addUtf8String(query.sql);
    msg.setLength();

    _socket.add(msg.buffer);

    _state = busy;
    query._state = _BUSY;
    _transactionState = unknown;
  }

  void _readRowDescription(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    _state = streaming;

    int count = _buffer.readInt16();
    var list = <_Column>[];

    for (int i = 0; i < count; i++) {
      var name = _buffer.readUtf8String(length); //TODO better maxSize.
      int fieldId = _buffer.readInt32();
      int tableColNo = _buffer.readInt16();
      int fieldType = _buffer.readInt32();
      int dataSize = _buffer.readInt16();
      int typeModifier = _buffer.readInt32();
      int formatCode = _buffer.readInt16();

      list.add(_Column(i, name, fieldId, tableColNo, fieldType, dataSize, typeModifier, formatCode));
    }

    final query = _query!;
    query._columnCount = count;
    query._columns = UnmodifiableListView(list);
    query._commandIndex++;

    query.addRowDescription();
  }

  void _readDataRow(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    int columns = _buffer.readInt16();
    for (var i = 0; i < columns; i++) {
      int size = _buffer.readInt32();
      _readColumnData(i, size);
    }
  }

  void _readColumnData(int index, int colSize) {

    assert(_buffer.bytesAvailable >= colSize);

    final query = _query!;
    if (index == 0)
      query._rowData = List<dynamic>.filled(query._columns!.length, null);
    final rowData = query._rowData!;

    if (colSize == -1) {
      rowData[index] = null;
    } else {
      var col = query._columns![index];
      if (col.isBinary) throw PostgresqlException(
          'Binary result set parsing is not implemented.', debugName);

      var str = _buffer.readUtf8StringN(colSize),
        value = _typeConverter.decode(str, col.fieldType,
            connectionName: _debugName);

      rowData[index] = value;
    }

    // If last column, then return the row.
    if (index == query._columnCount! - 1)
      query.addRow();
  }

  void _readCommandComplete(int msgType, int length) {

    assert(_buffer.bytesAvailable >= length);

    var commandString = _buffer.readUtf8String(length);
    int rowsAffected = int.tryParse(commandString.split(' ').last) ?? 0;

    final query = _query!;
    query._commandIndex++;
    query._rowsAffected = rowsAffected;
  }

  @override
  void close() {

    if (_state == closed)
      return;

    _state = closed;

    //Error-out the active query and any queued queries so listeners don't hang.
    _abortPendingQueries(PostgresqlException(
        'Connection closed before query could complete', debugName,
        exception: peConnectionClosed));

    try {
      var msg = MessageBuffer();
      msg.addByte(_MSG_TERMINATE);
      msg.addInt32(0);
      msg.setLength();
      _socket.add(msg.buffer);
      _socket.flush().whenComplete(_destroy);
      // Wait for socket flush to succeed or fail before closing the connection.
    } catch (e, st) {
      _messages.add(ClientMessageImpl(
          severity: 'WARNING',
          message: 'Exception while closing connection. Closed without sending '
            'terminate message.',
          connectionName: debugName,
          exception: e,
          stackTrace: st));
    }
  }

  void _destroy() {
    _state = closed;
    _socket.destroy();
    Timer.run(_messages.close);
  }
}


class _SaslAuthenticator {
  final SaslAuthenticator authenticator;

  _SaslAuthenticator(this.authenticator);

  void sasl(Socket socket, Buffer buffer, Uint8List bytesReceivedFromServer) {
    final bytesToSendToServer = authenticator.handleMessage(
      SaslMessageType.AuthenticationSASL,
      bytesReceivedFromServer);

     if (bytesToSendToServer == null) {
      throw PostgresqlException('KindSASL: No bytes to send', null,
          exception: peConnectionFailed); 
    }

    final mechanismName = authenticator.mechanism.name;
    final encodedMechanismName = utf8.encode(mechanismName);
    final length = bytesToSendToServer.length;
    // No Identifier bit + 4 byte counts (for whole length) + mechanism bytes + zero byte + 4 byte counts (for msg length) + msg bytes
    final totalLength = 4 + encodedMechanismName.length + 1 + 4 + length;

    final msg = MessageBuffer();
    msg.addByte(_MSG_PASSWORD);
    msg.addInt32(totalLength);
    msg.addUtf8String(mechanismName);
    msg.addInt32(length);
    msg.buffer.addAll(bytesToSendToServer);

    msg.setLength();
    socket.add(msg.buffer);
  }

  void saslContinue(Socket socket, Buffer buffer, Uint8List bytesReceivedFromServer) {
    final bytesToSendToServer = authenticator.handleMessage(
      SaslMessageType.AuthenticationSASLContinue,
      bytesReceivedFromServer);

    if (bytesToSendToServer == null) {
      throw PostgresqlException('KindSASLContinue: No bytes to send', null,
          exception: peConnectionFailed); 
    }

    final length = 4 + bytesToSendToServer.length;
    final msg = MessageBuffer();
    msg.addByte(_MSG_PASSWORD);
    msg.addInt32(length);
    msg.buffer.addAll(bytesToSendToServer);

    msg.setLength();
    socket.add(msg.buffer);
  }


  void saslFinal(Socket socket, Buffer buffer, Uint8List bytesReceivedFromServer) {
    authenticator.handleMessage(
      SaslMessageType.AuthenticationSASLFinal,
      bytesReceivedFromServer);
  }
}