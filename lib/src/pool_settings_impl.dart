library postgresql.pool.pool_settings_impl;

import 'package:postgresql2/pool.dart';
import 'package:postgresql2/postgresql.dart' as pg;

final PoolSettingsImpl _default = new PoolSettingsImpl(databaseUri: '');

class PoolSettingsImpl implements PoolSettings {
  
  PoolSettingsImpl({
      required this.databaseUri,
      String? poolName,
      this.minConnections: 5,
      this.maxConnections: 10,
      this.limitConnections: 0,
      void Function(int count)? this.onMaxConnection,
      this.startTimeout: const Duration(seconds: 30),
      this.stopTimeout: const Duration(seconds: 30),
      this.establishTimeout: const Duration(seconds: 30),
      this.connectionTimeout: const Duration(seconds: 30),
      this.idleTimeout: const Duration(minutes: 10),
      this.limitTimeout: const Duration(milliseconds: 700),
      this.maxLifetime: const Duration(minutes: 30),
      this.leakDetectionThreshold: null, // Disabled by default.
      this.testConnections: false,
      this.restartIfAllConnectionsLeaked: false,
      this.applicationName,
      this.timeZone})
        : this.poolName = poolName != null ? poolName : 'pgpool${_sequence++}';


 // Ugly work around for passing defaults from Pool constructor.
 factory PoolSettingsImpl.withDefaults({
        required String databaseUri,
        String? poolName,
        int? minConnections,
        int? maxConnections,
        int? limitConnections,
        void Function(int count)? onMaxConnection,
        Duration? startTimeout,
        Duration? stopTimeout,
        Duration? establishTimeout,
        Duration? connectionTimeout,
        Duration? idleTimeout,
        Duration? limitTimeout,
        Duration? maxLifetime,
        Duration? leakDetectionThreshold,
        bool? testConnections,
        bool? restartIfAllConnectionsLeaked,
        String? applicationName,
        String? timeZone}) {
  
   return new PoolSettingsImpl(
     databaseUri: databaseUri,
     poolName: poolName,
     minConnections: minConnections ?? _default.minConnections,
     maxConnections: maxConnections ?? _default.maxConnections,
     limitConnections: limitConnections ?? _default.limitConnections,
     onMaxConnection: onMaxConnection,
     startTimeout: startTimeout  ?? _default.startTimeout,
     stopTimeout: stopTimeout  ?? _default.stopTimeout,
     establishTimeout: establishTimeout  ?? _default.establishTimeout,
     connectionTimeout: connectionTimeout  ?? _default.connectionTimeout,
     idleTimeout: idleTimeout  ?? _default.idleTimeout,
     limitTimeout: limitTimeout  ?? _default.limitTimeout,
     maxLifetime: maxLifetime  ?? _default.maxLifetime,
     leakDetectionThreshold: leakDetectionThreshold  ?? _default.leakDetectionThreshold,
     testConnections: testConnections  ?? _default.testConnections,
     restartIfAllConnectionsLeaked: restartIfAllConnectionsLeaked  ?? _default.restartIfAllConnectionsLeaked,
     applicationName: applicationName,
     timeZone: timeZone); 
 }

  // Ids will be unique for this isolate.
  static int _sequence = 0;

  @override
  final String databaseUri;
  @override
  final String poolName;
  @override
  final int minConnections;
  @override
  final int maxConnections;
  @override
  final int limitConnections;
  @override
  final void Function(int count)? onMaxConnection;
  @override
  final Duration startTimeout;
  @override
  final Duration stopTimeout;
  @override
  final Duration establishTimeout;
  @override
  final Duration connectionTimeout;
  @override
  final Duration idleTimeout;
  @override
  final Duration limitTimeout;
  @override
  final Duration maxLifetime;
  @override
  final Duration? leakDetectionThreshold;
  @override
  final bool testConnections;
  @override
  final bool restartIfAllConnectionsLeaked;
  @override
  final String? applicationName;
  @override
  final String? timeZone;

  @override
  String toString()
  => 'PoolSettings ${new pg.Settings.fromUri(databaseUri)}';
}
