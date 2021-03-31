part of postgresql.impl;

class SettingsImpl implements Settings {
  String _host;
  int _port;
  String _user;
  String _password;
  String _database;
  bool _requireSsl;
  
  static const String DEFAULT_HOST = 'localhost';
  static const String HOST = 'host';
  static const String PORT = 'port';
  static const String USER = 'user';
  static const String PASSWORD = 'password';
  static const String DATABASE = 'database';
  
  SettingsImpl(this._host,
      this._port,
      this._user,
      this._password,
      this._database,
      {bool requireSsl: false})
    : _requireSsl = requireSsl;
  
  static _error(msg) => new PostgresqlException('Settings: $msg', null);

  factory SettingsImpl.fromUri(String uri) {
    var u = Uri.parse(uri);
    if (u.scheme != 'postgres' && u.scheme != 'postgresql')
      throw _error('Invalid uri: scheme must be `postgres` or `postgresql`.');

    if (u.userInfo == '')
      throw _error('Invalid uri: username must be specified.');

    var userInfo;
    if (u.userInfo.contains(':'))
      userInfo = u.userInfo.split(':');
    else userInfo = [u.userInfo, ''];

    if (!u.path.startsWith('/') || !(u.path.length > 1))
      throw _error('Invalid uri: `database name must be specified`.');

    final requireSsl = u.query.contains('sslmode=require');

    return new SettingsImpl(
        Uri.decodeComponent(u.host),
        u.port == 0 ? Settings.defaultPort : u.port,
        Uri.decodeComponent(userInfo[0]),
        Uri.decodeComponent(userInfo[1]),
        Uri.decodeComponent(u.path.substring(1)), // Remove preceding forward slash.
        requireSsl: requireSsl);
  }

  @override
  String get host => _host;
  @override
  int get port => _port;
  @override
  String get user => _user;
  @override
  String get password => _password;
  @override
  String get database => _database;
  @override
  bool get requireSsl => _requireSsl;

  @override
  String toUri() => new Uri(
        scheme: 'postgres',
        userInfo: _password == '' ? '$_user': '$_user:$_password',
        host: _host,
        port: _port,
        path: _database,
        query: requireSsl ? '?sslmode=require' : null).toString();

  @override
  String toString()
    => "Settings {host: $_host, port: $_port, user: $_user, database: $_database}";
}
