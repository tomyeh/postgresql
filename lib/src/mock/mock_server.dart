part of postgresql.mock;

class MockServerBackendImpl implements Backend {
  MockServerBackendImpl() {
    mocket.onClose = () {
      _isClosed = true;
      log.add(new Packet(clientClosed, []));
    };

    mocket.onDestroy = () {
      _isClosed = true;
      _isDestroyed = true;
      log.add(new Packet(clientDestroyed, []));
    };

    mocket.onAdd = (Uint8List data) {
      received.add(data);
      log.add(new Packet(toServer, data));
      if (_waitForClient != null) {
        _waitForClient.complete();
        _waitForClient = null;
      }
    };

    mocket.onError = (err, [st]) {
      throw err;
    };
  }

  final Mocket mocket = new Mocket();

  final List<Packet> log = new List<Packet>();
  final List<Uint8List> received = new List<Uint8List>();

  bool _isClosed = true;
  bool _isDestroyed = true;
  bool get isClosed => _isClosed;
  bool get isDestroyed => _isDestroyed;

  /// Clear out received data.
  void clear() {
    received.clear();
  }

  /// Server closes the connection to client.
  void close() {
    log.add(new Packet(serverClosed, []));
    _isClosed = true;
    _isDestroyed = true;
    mocket.close();
  }

  Completer _waitForClient;

  /// Wait for the next packet to arrive from the client.
  Future waitForClient() {
    if (_waitForClient == null) _waitForClient = new Completer();
    return _waitForClient.future;
  }

  /// Send data over the socket from the mock server to the client listening
  /// on the socket.
  void sendToClient(Uint8List data) {
    log.add(new Packet(toClient, data));
    mocket._controller.add(data);
  }

  void socketException(String msg) {
    log.add(new Packet(socketError, []));
    mocket._controller.addError(new SocketException(msg));
  }
}

class MockServerImpl implements MockServer {
  MockServerImpl();

  Future<pg.Connection> connect() =>
      ConnectionImpl.connect('postgres://testdb:password@localhost:5433/testdb',
          mockSocketConnect: (host, port) => new Future(() => _startBackend()));

  stop() {}

  final List<Backend> backends = <Backend>[];

  Mocket _startBackend() {
    var backend = new MockServerBackendImpl();
    backends.add(backend);

    if (_waitForConnect != null) {
      _waitForConnect.complete(backend);
      _waitForConnect = null;
    }

    return backend.mocket;
  }

  Completer<Backend> _waitForConnect;

  /// Wait for the next client to connect.
  Future<Backend> waitForConnect() {
    if (_waitForConnect == null) _waitForConnect = new Completer();
    return _waitForConnect.future;
  }
}

class Mocket extends StreamView<Uint8List> implements Socket {
  factory Mocket() => new Mocket._private(new StreamController<Uint8List>());

  Mocket._private(StreamController<Uint8List> ctl)
      : _controller = ctl,
        super(ctl.stream);

  final StreamController<Uint8List> _controller;

  bool _isDone = false;

  Function onClose;
  Function onDestroy;
  Function onAdd;
  Function onError;

  Future<Socket> close() {
    _isDone = true;
    onClose();
    return new Future.value();
  }

  void destroy() {
    _isDone = true;
    onDestroy();
  }

  Uint8List getRawOption(RawSocketOption option) {return null;}

  void setRawOption(RawSocketOption option) {}

  void add(List<int> data) => onAdd(data);

  void addError(error, [StackTrace stackTrace]) => onError(error, stackTrace);

  Future addStream(Stream<List<int>> stream) {
    throw new UnimplementedError();
  }

  @override
  Future get done => new Future.value(_isDone);

  InternetAddress get address => throw new UnimplementedError();
  get encoding => throw new UnimplementedError();
  void set encoding(_encoding) => throw new UnimplementedError();
  Future flush() => new Future.value(null);
  int get port => throw new UnimplementedError();
  InternetAddress get remoteAddress => throw new UnimplementedError();
  int get remotePort => throw new UnimplementedError();
  bool setOption(SocketOption option, bool enabled) =>
      throw new UnimplementedError();
  void write(Object obj) => throw new UnimplementedError();

  void writeAll(Iterable objects, [String separator = ""]) =>
      throw new UnimplementedError();
  void writeCharCode(int charCode) => throw new UnimplementedError();
  void writeln([Object obj = ""]) => throw new UnimplementedError();
}
