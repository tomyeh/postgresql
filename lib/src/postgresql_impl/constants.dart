part of postgresql.impl;

const int _QUEUED = 1;
const int _BUSY = 6;
const int _STREAMING = 7;
const int _DONE = 8;

const int _I = 73;
const int _T = 84;
const int _E = 69;

//const int _t = 116;
//const int _M = 77;
const int _S = 83;

const int _PROTOCOL_VERSION = 196608;

const int _AUTH_TYPE_MD5 = 5;
const int _AUTH_TYPE_OK = 0;

// Messages sent by client (Frontend).
//const int _MSG_STARTUP = -1; // Fake message type as StartupMessage has no type in the header.
const int _MSG_PASSWORD = 112; // 'p'
const int _MSG_QUERY = 81; // 'Q'
const int _MSG_TERMINATE = 88; // 'X'

// Message types sent by the server.
const int _MSG_AUTH_REQUEST = 82; //'R'.charCodeAt(0);
const int _MSG_ERROR_RESPONSE = 69; //'E'.charCodeAt(0);
const int _MSG_BACKEND_KEY_DATA = 75; //'K'.charCodeAt(0);
const int _MSG_PARAMETER_STATUS = 83; //'S'.charCodeAt(0);
const int _MSG_NOTICE_RESPONSE = 78; //'N'.charCodeAt(0);
const int _MSG_NOTIFICATION_RESPONSE = 65; //'A'.charCodeAt(0);
//const int _MSG_BIND = 66; //'B'.charCodeAt(0);
//const int _MSG_BIND_COMPLETE = 50; //'2'.charCodeAt(0);
//const int _MSG_CLOSE_COMPLETE = 51; //'3'.charCodeAt(0);
const int _MSG_COMMAND_COMPLETE = 67; //'C'.charCodeAt(0);
const int _MSG_COPY_DATA = 100; //'d'.charCodeAt(0);
//const int _MSG_COPY_DONE = 99; //'c'.charCodeAt(0);
//const int _MSG_COPY_IN_RESPONSE = 71; //'G'.charCodeAt(0);
//const int _MSG_COPY_OUT_RESPONSE = 72; //'H'.charCodeAt(0);
//const int _MSG_COPY_BOTH_RESPONSE = 87; //'W'.charCodeAt(0);
const int _MSG_DATA_ROW = 68; //'D'.charCodeAt(0);
const int _MSG_EMPTY_QUERY_REPONSE = 73; //'I'.charCodeAt(0);
const int _MSG_FUNCTION_CALL_RESPONSE = 86; //'V'.charCodeAt(0);
//const int _MSG_NO_DATA = 110; //'n'.charCodeAt(0);
//const int _MSG_PARAMETER_DESCRIPTION = 116; //'t'.charCodeAt(0);
//const int _MSG_PARSE_COMPLETE = 49; //'1'.charCodeAt(0);
//const int _MSG_PORTAL_SUSPENDED = 115; //'s'.charCodeAt(0);
const int _MSG_READY_FOR_QUERY = 90; //'Z'.charCodeAt(0);
const int _MSG_ROW_DESCRIPTION = 84; //'T'.charCodeAt(0);

String _itoa(int c) {
  try {
    return new String.fromCharCodes([c]);
  } catch (ex) {
    return 'Invalid';
  }
}

String _authTypeAsString(int authType) {
  const unknown = 'Unknown';
  const names = const <String> ['Authentication OK',
                                unknown,
                                'Kerberos v5',
                                'cleartext password',
                                unknown,
                                'MD5 password',
                                'SCM credentials',
                                'GSSAPI',
                                'GSSAPI or SSPI authentication data',
                                'SSPI'];
  var type = unknown;
  if (authType > 0 && authType < names.length)
    type = names[authType];
  return type;
}

/// Constants for postgresql datatypes
/// Ref: https://jdbc.postgresql.org/development/privateapi/constant-values.html
/// Also: select typname, typcategory, typelem, typarray from pg_type where typname LIKE '%int%'
const int
  _BIT = 1560,
  _BIT_ARRAY = 1561,
  _BOOL = 16,
  _BOOL_ARRAY = 1000,
//  _BOX = 603,
  _BPCHAR = 1042,
  _BPCHAR_ARRAY = 1014,
  _BYTEA = 17,
  _BYTEA_ARRAY = 1001,
  _CHAR = 18,
  _CHAR_ARRAY = 1002,
  _DATE = 1082,
  _DATE_ARRAY = 1182,
  _FLOAT4 = 700,
  _FLOAT4_ARRAY = 1021,
  _FLOAT8 = 701,
  _FLOAT8_ARRAY = 1022,
  _INT2 = 21,
  _INT2_ARRAY = 1005,
  _INT4 = 23,
  _INT4_ARRAY = 1007,
  _INT8 = 20,
  _INT8_ARRAY = 1016,
  _INTERVAL = 1186,
  _INTERVAL_ARRAY = 1187,
  _JSON = 114,
  _JSON_ARRAY = 199,
  _JSONB = 3802,
  _JSONB_ARRAY = 3807,
  _MONEY = 790,
  _MONEY_ARRAY = 791,
  _NAME = 19,
  _NAME_ARRAY = 1003,
  _NUMERIC = 1700,
  _NUMERIC_ARRAY = 1231,
  _OID = 26,
  _OID_ARRAY = 1028,
  //_POINT = 600,
  _TEXT = 25,
  _TEXT_ARRAY = 1009,
  _TIME = 1083,
  _TIME_ARRAY = 1183,
  _TIMESTAMP = 1114,
  _TIMESTAMP_ARRAY = 1115,
  _TIMESTAMPZ = 1184,
  _TIMESTAMPZ_ARRAY = 1185,
  _TIMETZ = 1266,
  _TIMETZ_ARRAY = 1270,
  //_UNSPECIFIED = 0,
  _UUID = 2950,
  _UUID_ARRAY = 2951,
  _VARBIT = 1562,
  _VARBIT_ARRAY = 1563,
  _VARCHAR = 1043,
  _VARCHAR_ARRAY = 1015,
  //_VOID = 2278,
  _XML = 142,
  _XML_ARRAY = 143;
