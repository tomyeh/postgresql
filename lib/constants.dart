/// Export shorthand constants for enums at top-level.
library postgresql.constants;

import 'package:postgresql2/postgresql.dart';
import 'package:postgresql2/pool.dart';

const ConnectionState notConnected = ConnectionState.notConnected;
const ConnectionState socketConnected = ConnectionState.socketConnected;
const ConnectionState authenticating = ConnectionState.authenticating;
const ConnectionState authenticated = ConnectionState.authenticated;
const ConnectionState idle = ConnectionState.idle;
const ConnectionState busy = ConnectionState.busy;
const ConnectionState streaming = ConnectionState.streaming;
const ConnectionState closed = ConnectionState.closed;

const Isolation readCommitted = Isolation.readCommitted;
const Isolation repeatableRead = Isolation.repeatableRead;
const Isolation serializable = Isolation.serializable;

const TransactionState unknown = TransactionState.unknown;
const TransactionState none = TransactionState.none;
const TransactionState begun = TransactionState.begun;
const TransactionState error = TransactionState.error;

const PoolState initial = PoolState.initial;
const PoolState starting = PoolState.starting;
const PoolState startFailed = PoolState.startFailed;
const PoolState running = PoolState.running;
const PoolState stopping = PoolState.stopping;
const PoolState stopped = PoolState.stopped;

/// Errors used in [PostgresqlException.exception] when [Pool.connect] failed.
const
    peConnectionTimeout = 4001,
    pePoolStopped = 4002,
    peConnectionClosed = 4003,
    peConnectionFailed = 40004; //miscellaneous connection errors (excluding SQL statement errors)

// pg '-infinity' and 'infinity' representation
// DateTimes can represent time values that are at a distance of at most 100,000,000
// days from epoch (1970-01-01 UTC): -271821-04-20 to 275760-09-13.
DateTime pgMinDateTime = DateTime.utc(-271821,04,20);
DateTime pgMaxDateTime = DateTime.utc(275760,09,13);
