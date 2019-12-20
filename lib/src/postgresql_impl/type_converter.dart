part of postgresql.impl;

const int _apos = 39;
const int _return = 13;
const int _newline = 10;
const int _backslash = 92;
const int _null = 0;

final _escapeRegExp = new RegExp(r"['\r\n\\\u0000]"); //detect unsupported null

class RawTypeConverter extends DefaultTypeConverter {
   String encode(value, String type, {getConnectionName()})
    => encodeValue(value, type);
   
   Object decode(String value, int pgType, {bool isUtcTimeZone: false,
     getConnectionName()}) => value;
}

String encodeString(String s, {bool trimNull: false}) {
  if (s == null) return ' null ';

  var escaped = s.replaceAllMapped(_escapeRegExp, (m) {
   switch (s.codeUnitAt(m.start)) {
     case _apos: return r"\'";
     case _return: return r'\r';
     case _newline: return r'\n';
     case _backslash: return r'\\';
     case _null:
      if (!trimNull)
        throw new PostgresqlException('Not allowed: null character', '');
      return '';
   }
   throw StateError("$m");
 });

  return " E'$escaped' ";
}

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
  
    throwError() => throw _error('Invalid runtime type and type modifier '
        'combination (${value.runtimeType} to $type).', getConnectionName);
  
    if (value == null)
      return 'null';
  
    if (type != null)
      type = type.toLowerCase();
  
    if (type == 'text' || type == 'string') {
      if (value is! String) throwError(); //play safe
      return encodeString(value);
    }
  
    if (type == 'integer'
        || type == 'smallint'
        || type == 'bigint'
        || type == 'serial'
        || type == 'bigserial'
        || type == 'int') {
      if (value is! int) throwError();
      return encodeNumber(value);
    }
  
    if (type == 'real'
        || type == 'double'
        || type == 'num'
        || type == 'number') {
      if (value is! num) throwError();
      return encodeNumber(value);
    }
  
    // TODO numeric, decimal
  
    if (type == 'boolean' || type == 'bool') {
      if (value is! bool) throwError();
      return value.toString();
    }
  
    if (type == 'timestamp' || type == 'timestamptz' || type == 'datetime') {
      if (value is! DateTime) throwError();
      return encodeDateTime(value, isDateOnly: false);
    }
  
    if (type == 'date') {
      if (value is! DateTime) throwError();
      return encodeDateTime(value, isDateOnly: true);
    }
  
    if (type == 'json' || type == 'jsonb')
      return encodeString(json.encode(value));
  
  //  if (type == 'bytea') {
  //    if (value is! List<int>) throwError();
  //    return encodeBytea(value);
  //  }
  //
  //  if (type == 'array') {
  //    if (value is! List) throwError();
  //    return encodeArray(value);
  //  }
  
    throw _error('Unknown type name: $type.', getConnectionName);
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
  
    if (value is Map)
      return encodeString(json.encode(value));
  
    if (value is List)
      return encodeArray(value);
  
    throw _error('Unsupported runtime type as query parameter '
        '(${value.runtimeType}).', getConnectionName);
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
