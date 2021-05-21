part of postgresql.impl;


class ClientMessageImpl implements ClientMessage {

  ClientMessageImpl(
      {this.isError: false,
       required this.severity,
       required this.message,
       this.connectionName,
       Object? this.exception,
       this.stackTrace}) {
    if (severity != 'ERROR' && severity != 'WARNING' && severity != 'DEBUG')
      throw new ArgumentError.notNull('severity');
  }

  @override
  final bool isError;
  @override
  final String severity;
  @override
  final String message;
  @override
  final String? connectionName;
  @override
  final Object? exception;
  @override
  final StackTrace? stackTrace;
  
  @override
  String toString() => connectionName == null
        ? '$severity $message'
        : '$severity $message #$connectionName';
}

class ServerMessageImpl implements ServerMessage {

  ServerMessageImpl(this.isError, Map<String,String> fields, [this.connectionName])
      : fields = new UnmodifiableMapView<String,String>(fields),
        severity = fields['S'],
        code = fields['C'],
        message = fields['M'] ?? '?';

  @override
  final bool isError;
  @override
  final String? connectionName;
  @override
  final Map<String,String> fields;
  
  @override
  final String? severity;
  @override
  final String? code;
  @override
  final String message;

  @override
  String? get detail => fields['D'];
  @override
  String? get hint => fields['H'];
  @override
  String? get position => fields['P'];
  @override
  String? get internalPosition => fields['p'];
  @override
  String? get internalQuery => fields['q'];
  @override
  String? get where => fields['W'];
  @override
  String? get schema => fields['s'];
  @override
  String? get table => fields['t'];
  @override
  String? get column => fields['c'];
  @override
  String? get dataType => fields['d'];
  @override
  String? get constraint => fields['n'];
  @override
  String? get file => fields['F'];
  @override
  String? get line => fields['L'];
  @override
  String? get routine => fields['R'];
  
  @override
  String toString() => connectionName == null
      ? '$severity $code $message'
      : '$severity $code $message #$connectionName';
}
