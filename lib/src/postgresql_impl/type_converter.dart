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
   
   Object decode(String value, int pgType, {bool isUtcTimeZone: false,
     getConnectionName()}) => value;
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
   
  Object decode(String value, int pgType, {bool isUtcTimeZone: false,
    getConnectionName()}) => decodeValue(value, pgType, 
        isUtcTimeZone: isUtcTimeZone, getConnectionName: getConnectionName);

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
        if (value is List<int>) return encodeBytea(value);
        break;

      default:
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
  
  String encodeArray(List value) {
    //TODO implement postgresql array types
    throw _error('Postgresql array types not implemented yet. '
        'Pull requests welcome ;)', null);
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
  
  Object decodeValue(String value, int pgType,
                     {bool isUtcTimeZone, getConnectionName()}) {
  
    switch (pgType) {
  
      case _PG_BOOL:
        return value == 't';
  
      case _PG_INT2: // smallint
      case _PG_INT4: // integer
      case _PG_INT8: // bigint
        return int.parse(value);
  
      case _PG_FLOAT4: // real
      case _PG_FLOAT8: // double precision
        return double.parse(value);
  
      case _PG_TIMESTAMP:
      case _PG_TIMESTAMPZ:
      case _PG_DATE:
        return decodeDateTime(value, pgType,
                  isUtcTimeZone: isUtcTimeZone, getConnectionName: getConnectionName);
  
      case _PG_JSON:
      case _PG_JSONB:
        return json.decode(value);
  
      case _PG_NUMERIC:
        try {
          return BigInt.parse(value);
        } catch (_) {
        }
        return value;

      // Not implemented yet - return a string.
      case _PG_MONEY:
      case _PG_TIMETZ:
      case _PG_TIME:
      case _PG_INTERVAL:
  
      //TODO arrays
      //TODO binary bytea
  
      default:
        // Return a string for unknown types. The end user can parse this.
        return value;
    }
  }

  DateTime decodeDateTime(String value, int pgType, {bool isUtcTimeZone, getConnectionName()}) {
    // Built in Dart dates can either be local time or utc. Which means that the
    // the postgresql timezone parameter for the connection must be either set
    // to UTC, or the local time of the server on which the client is running.
    // This restriction could be relaxed by using a more advanced date library
    // capable of creating DateTimes for a non-local time zone.

    if (value == 'infinity' || value == '-infinity') {
      throw _error('Server returned a timestamp with value '
          '"$value", this cannot be represented as a dart date object, if '
          'infinity values are required, rewrite the sql query to cast the '
          'value to a string, i.e. col::text.', getConnectionName);
    }

    var formattedValue = value;

    // Postgresql uses a BC suffix rather than a negative prefix as in ISO8601.
    if (value.endsWith(' BC')) formattedValue = '-' + value.substring(0, value.length - 3);

    if (pgType == _PG_TIMESTAMP) {
        formattedValue += 'Z';
    } else if (pgType == _PG_TIMESTAMPZ) {
      // PG will return the timestamp in the connection's timezone. The resulting DateTime.parse will handle accordingly.
    } else if (pgType == _PG_DATE) {
      formattedValue = formattedValue + 'T00:00:00Z';
    }

    return DateTime.parse(formattedValue);
  }
  
}
