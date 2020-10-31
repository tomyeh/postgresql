part of postgresql.impl;

/// Map of characters to escape.
const escapes = const {
  "'": r"\'", "\r": r"\r", "\n": r"\n", r"\": r"\\",
  "\t": r"\t", "\b": r"\b", "\f": r"\f", "\u0000": "",
};
/// Characters that will be escapes.
const escapePattern = r"'\r\n\\\t\b\f\u0000"; //detect unsupported null
final _escapeRegExp = new RegExp("[$escapePattern]");

class RawTypeConverter extends DefaultTypeConverter {
  String encode(value, String type, {getConnectionName()})
  => encodeValue(value, type);
   
  decode(String value, int pgType, {getConnectionName()}) => value;
}

/// Encodes the given string ([s]) into the format: ` E'xxx'`
/// 
/// > Note: the null character (`\u0000`) will be removed, since
/// > PostgreSql won't accept it.
String encodeString(String s) {
  if (s == null) return ' null ';

  var escaped = s.replaceAllMapped(_escapeRegExp, _escape);
  return " E'$escaped' ";
}
String _escape(Match m) => escapes[m[0]];

class DefaultTypeConverter implements TypeConverter {
    
  String encode(value, String type, {getConnectionName()}) 
    => encodeValue(value, type, getConnectionName: getConnectionName);
   
  decode(String value, int pgType, {getConnectionName()})
  => decodeValue(value, pgType, getConnectionName: getConnectionName);

  PostgresqlException _error(String msg, getConnectionName()) {
    var name = getConnectionName == null ? null : getConnectionName();
    return new PostgresqlException(msg, name);
  }
  
  String encodeValue(value, String type, {getConnectionName()}) {
    if (type == null)
      return encodeValueDefault(value, getConnectionName: getConnectionName);
    if (value == null)
      return 'null';

    switch (type) {
      case 'text': case 'string':
        if (value is String)
          return encodeString(value);
        break;

      case 'integer': case 'smallint':
      case 'bigint': case 'serial':
      case 'bigserial': case 'int':
        if (value is int || value is BigInt)
          return encodeNumber(value);
        break;

      case 'real': case 'double':
      case 'num': case 'number':
        if (value is num)
          return encodeNumber(value);
        break;

    // TODO numeric, decimal

      case 'boolean': case 'bool':
        if (value is bool)
          return value.toString();
        break;

      case 'timestamp': case 'timestamptz': case 'datetime':
        if (value is DateTime)
          return encodeDateTime(value, isDateOnly: false);
        break;

      case 'date':
        if (value is DateTime)
          return encodeDateTime(value, isDateOnly: true);
        break;
  
      case 'json': case 'jsonb':
        return encodeJson(value);
  
      case 'array':
        if (value is List)
          return encodeArray(value);
        break;

      case 'bytea':
        if (value is List<int>)
          return encodeBytea(value);
        break;

      default:
        if (type.endsWith('_array'))
          return encodeArray(value, pgType: type.substring(0, type.length - 6));

        final t = type.toLowerCase(); //backward compatible
        if (t != type)
          return encodeValue(value, t, getConnectionName: getConnectionName);

        throw _error('Unknown type name: $type.', getConnectionName);
    }

    throw _error('Invalid runtime type and type modifier: '
        '${value.runtimeType} to $type.', getConnectionName);
  }
  
  // Unspecified type name. Use default type mapping.
  String encodeValueDefault(value, {getConnectionName()}) {
    if (value == null)
      return 'null';
    if (value is num)
      return encodeNumber(value);
    if (value is String)
      return encodeString(value);
    if (value is DateTime)
      return encodeDateTime(value, isDateOnly: false);
    if (value is bool || value is BigInt)
      return value.toString();
    if (value is List)
      return encodeArray(value);
    return encodeJson(value);
  }
  
  String encodeNumber(num n) {
    if (n.isNaN) return "'nan'";
    if (n == double.infinity) return "'infinity'";
    if (n == double.negativeInfinity) return "'-infinity'";
    return n.toString();
  }
  
  String encodeArray(List value, {String pgType}) {
    final buf = new StringBuffer('array[');
    for (final v in value) {
      if (buf.length > 6) buf.write(',');
      buf.write(encodeValueDefault(v));
    }
    buf.write(']');
    if (pgType != null) buf..write('::')..write(pgType)..write('[]');
    return buf.toString();
  }

  String encodeDateTime(DateTime datetime, {bool isDateOnly}) {
      if (datetime == null)
      return 'null';

    var string = datetime.toIso8601String();

    if (isDateOnly) {
      string = string.split("T").first;
    } else {

      // ISO8601 UTC times already carry Z, but local times carry no timezone info
      // so this code will append it.
      if (!datetime.isUtc) {
        var timezoneHourOffset = datetime.timeZoneOffset.inHours;
        var timezoneMinuteOffset = datetime.timeZoneOffset.inMinutes % 60;

        // Note that the sign is stripped via abs() and appended later.
        var hourComponent = timezoneHourOffset.abs().toString().padLeft(2, "0");
        var minuteComponent = timezoneMinuteOffset.abs().toString().padLeft(2, "0");

        if (timezoneHourOffset >= 0) {
          hourComponent = "+${hourComponent}";
        } else {
          hourComponent = "-${hourComponent}";
        }

        var timezoneString = [hourComponent, minuteComponent].join(":");
        string = [string, timezoneString].join("");
      }
    }

    if (string.substring(0, 1) == "-") {
      // Postgresql uses a BC suffix for dates rather than the negative prefix returned by
      // dart's ISO8601 date string.
      string = string.substring(1) + " BC";
    } else if (string.substring(0, 1) == "+") {
      // Postgresql doesn't allow leading + signs for 6 digit dates. Strip it out.
      string = string.substring(1);
    }

    return "'${string}'";
  }

  String encodeJson(value) => encodeString(jsonEncode(value));

  // See http://www.postgresql.org/docs/9.0/static/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE
  String encodeBytea(List<int> value) {
    //var b64String = ...;
    //return " decode('$b64String', 'base64') ";
  
    throw _error('bytea encoding not implemented. Pull requests welcome ;)', null);
  }
  
  decodeValue(String value, int pgType, {getConnectionName()}) {
    switch (pgType) {
      case _BOOL:
        return value == 't';

      case _INT2: // smallint
      case _INT4: // integer
      case _INT8: // bigint
        return int.parse(value);

      case _FLOAT4: // real
      case _FLOAT8: // double precision
        return double.parse(value);
  
      case _TIMESTAMP:
      case _TIMESTAMPZ:
      case _DATE:
        return decodeDateTime(value, pgType, getConnectionName: getConnectionName);

      case _JSON:
      case _JSONB:
        return jsonDecode(value);

      case _NUMERIC:
        try {
          return BigInt.parse(value);
        } catch (_) {
        }
        return value;

      //TODO binary bytea
  
      // Not implemented yet - return a string.
      //case _MONEY:
      //case _TIMETZ:
      //case _TIME:
      //case _INTERVAL:

      default:
        final scalarType = _arrayTypes[pgType];
        if (scalarType != null)
          return decodeArray(value, scalarType, getConnectionName: getConnectionName);

        // Return a string for unknown types. The end user can parse this.
        return value;
    }
  }
  static const _arrayTypes = {
    _BIT_ARRAY: _BIT,
    _BOOL_ARRAY: _BOOL,
    _BPCHAR_ARRAY: _BPCHAR,
    _BYTEA_ARRAY: _BYTEA,
    _CHAR_ARRAY: _CHAR,
    _DATE_ARRAY: _DATE,
    _FLOAT4_ARRAY: _FLOAT4,
    _FLOAT8_ARRAY: _FLOAT8,
    _INT2_ARRAY: _INT2,
    _INT4_ARRAY: _INT4,
    _INT8_ARRAY: _INT8,
    _INTERVAL_ARRAY: _INTERVAL,
    _JSON_ARRAY: _JSON,
    _JSONB_ARRAY: _JSONB,
    _MONEY_ARRAY: _MONEY,
    _NAME_ARRAY: _NAME,
    _NUMERIC_ARRAY: _NUMERIC,
    _OID_ARRAY: _OID,
    _TEXT_ARRAY: _TEXT,
    _TIME_ARRAY: _TIME,
    _TIMESTAMP_ARRAY: _TIMESTAMP,
    _TIMESTAMPZ_ARRAY: _TIMESTAMPZ,
    _TIMETZ_ARRAY: _TIMETZ,
    _UUID_ARRAY: _UUID,
    _VARBIT_ARRAY: _VARBIT,
    _VARCHAR_ARRAY: _VARCHAR,
    _XML_ARRAY: _XML,
  };

  /// Decodes [value] into a [DateTime] instance.
  /// 
  /// Note: it will convert it to local time (via [DateTime.toLocal])
  DateTime decodeDateTime(String value, int pgType, {getConnectionName()}) {
    // Built in Dart dates can either be local time or utc. Which means that the
    // the postgresql timezone parameter for the connection must be either set
    // to UTC, or the local time of the server on which the client is running.
    // This restriction could be relaxed by using a more advanced date library
    // capable of creating DateTimes for a non-local time zone.

    if (value == 'infinity' || value == '-infinity')
      throw _error('A timestamp value "$value", cannot be represented '
          'as a Dart object.', getConnectionName);
          //if infinity values are required, rewrite the sql query to cast
          //the value to a string, i.e. your_column::text.

    var formattedValue = value;

    // Postgresql uses a BC suffix rather than a negative prefix as in ISO8601.
    if (value.endsWith(' BC')) formattedValue = '-' + value.substring(0, value.length - 3);

    if (pgType == _TIMESTAMP) {
      formattedValue += 'Z';
    } else if (pgType == _TIMESTAMPZ) {
      // PG will return the timestamp in the connection's timezone. The resulting DateTime.parse will handle accordingly.
    } else if (pgType == _DATE) {
      formattedValue = formattedValue + 'T00:00:00Z';
    }

    return DateTime.parse(formattedValue).toLocal();
  }

  /// Decodes an array value, [value]. Each item of it is [pgType].
  decodeArray(String value, int pgType, {getConnectionName()}) {
    final len = value.length - 2;
    assert(value.codeUnitAt(0) == $lbrace && value.codeUnitAt(len + 1) == $rbrace);
    if (len <= 0) return [];
    value = value.substring(1, len + 1);

    if (const {_TEXT, _CHAR, _VARCHAR, _NAME}.contains(pgType)) {
      final result = [];
      for (int i = 0; i < len; ++i) {
        if (value.codeUnitAt(i) == $quot) {
          final buf = <int>[];
          for (;;) {
            final cc = value.codeUnitAt(++i);
            if (cc == $quot) {
              result.add(new String.fromCharCodes(buf));
              ++i;
              assert(i >= len || value.codeUnitAt(i) == $comma);
              break;
            }
            if (cc == $backslash) buf.add(value.codeUnitAt(++i));
            else buf.add(cc);
          }
        } else { //not quoted
          for (int j = i;; ++j) {
            if (j >= len || value.codeUnitAt(j) == $comma) {
              final v = value.substring(i, j);
              result.add(v == 'NULL' ? null: v);
              i = j;
              break;
            }
          }
        }
      }
      return result;
    }

    if (const {_JSON, _JSONB}.contains(pgType))
      return jsonDecode('[$value]');

    final result = [];
    for (final v in value.split(','))
      result.add(v == 'NULL' ? null:
          decodeValue(v, pgType, getConnectionName: getConnectionName));
    return result;
  }
}
