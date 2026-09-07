import 'package:test/test.dart';
import 'package:postgresql2/postgresql.dart';

main() {

  final tc = new TypeConverter() as DefaultTypeConverter;

  test('String escaping', () {
    expect(tc.encodeValue("bob", "json"), equals(' E\'"bob"\' '));

    expect(tc.encodeValue('bob', null), equals(" E'bob' "));
    expect(tc.encodeValue('bo\nb', null), equals(r" E'bo\nb' "));
    expect(tc.encodeValue('bo\rb', null), equals(r" E'bo\rb' "));
    expect(tc.encodeValue(r'bo\b', null), equals(r" E'bo\\b' "));

    expect(tc.encodeValue(r"'", null), equals(r" E'\'' "));
    expect(tc.encodeValue(r" '' ", null), equals(r" E' \'\' ' "));
    expect(tc.encodeValue(r"\''", null), equals(r" E'\\\'\'' "));
  });



// Timezone offsets
  //FIXME check that timezone offsets match the current system timezone offset.
  // Example strings that postgres may send.
//  "2001-02-03 04:05:06.123-07"
//  "2001-02-03 04:05:06-07"
//  "2001-02-03 04:05:06-07:42"
//  "2001-02-03 04:05:06-07:30:09"
//  "2001-02-03 04:05:06+07"
//  "0010-02-03 04:05:06.123-07 BC"

//  Also consider that some Dart datetimes will not be able to be represented
// in postgresql timestamps. i.e. pre 4713 BC or post 294276 AD. Perhaps just
// send these dates and rely on the database to return an error.


  test('encode datetime', () {
    // Get users current timezone
    var tz = new DateTime(2001, 2, 3).timeZoneOffset;
    var tzoff = "${tz.isNegative ? '-' : '+'}"
      "${tz.inHours.toString().padLeft(2, '0')}"
      ":${(tz.inSeconds % 60).toString().padLeft(2, '0')}";

    var data = [
      "2001-02-03T00:00:00.000$tzoff",      new DateTime(2001, DateTime.february, 3),
      "2001-02-03T04:05:06.000$tzoff",      new DateTime(2001, DateTime.february, 3, 4, 5, 6, 0),
      "2001-02-03T04:05:06.999$tzoff",      new DateTime(2001, DateTime.february, 3, 4, 5, 6, 999),
      "0010-02-03T04:05:06.123$tzoff BC",   new DateTime(-10, DateTime.february, 3, 4, 5, 6, 123),
      "0010-02-03T04:05:06.000$tzoff BC",   new DateTime(-10, DateTime.february, 3, 4, 5, 6, 0),
      "012345-02-03T04:05:06.000$tzoff BC",  new DateTime(-12345, DateTime.february, 3, 4, 5, 6, 0),
      "012345-02-03T04:05:06.000$tzoff",     new DateTime(12345, DateTime.february, 3, 4, 5, 6, 0)
    ];
    var tc = new TypeConverter();
    for (int i = 0; i < data.length; i += 2) {
      expect(tc.encode(data[i + 1], null), equals("'${data[i]}'"));
    }
  });

  test('encode date', () {
    var data = [
      "2001-02-03",     new DateTime(2001, DateTime.february, 3),
      "2001-02-03",     new DateTime(2001, DateTime.february, 3, 4, 5, 6, 0),
      "2001-02-03",     new DateTime(2001, DateTime.february, 3, 4, 5, 6, 999),
      "0010-02-03 BC",  new DateTime(-10, DateTime.february, 3, 4, 5, 6, 123),
      "0010-02-03 BC",  new DateTime(-10, DateTime.february, 3, 4, 5, 6, 0),
      "012345-02-03 BC", new DateTime(-12345, DateTime.february, 3, 4, 5, 6, 0),
      "012345-02-03",    new DateTime(12345, DateTime.february, 3, 4, 5, 6, 0),
    ];
    var tc = new TypeConverter();
    for (int i = 0; i < data.length; i += 2) {
      var str = data[i];
      var dt = data[i + 1];
      expect(tc.encode(dt, 'date'), equals("'$str'"));
    }
  });

  test('encode double', () {
    var data = [
      "'nan'", double.nan,
      "'infinity'", double.infinity,
      "'-infinity'", double.negativeInfinity,
      "1.7976931348623157e+308", double.maxFinite,
      "5e-324", double.minPositive,
      "-0.0", -0.0,
      "0.0", 0.0
    ];
    var tc = new TypeConverter();
    for (int i = 0; i < data.length; i += 2) {
      var str = data[i];
      var dt = data[i + 1];
      expect(tc.encode(dt, null), equals(str));
      expect(tc.encode(dt, 'real'), equals(str));
      expect(tc.encode(dt, 'double'), equals(str));
    }

    expect(tc.encode(null, 'real'), equals('null'));
  });

  /*test('encode int', () {
    var tc = new TypeConverter();
    expect(() => tc.encode(double.nan, 'integer'), throws);
    expect(() => tc.encode(double.infinity, 'integer'), throws);
    expect(() => tc.encode(1.0, 'integer'), throws);

    expect(tc.encode(1, 'integer'), equals('1'));
    expect(tc.encode(1, null), equals('1'));

    expect(tc.encode(null, 'integer'), equals('null'));
  });*/

  test('encode bool', () {
    var tc = new TypeConverter();
    expect(tc.encode(null, 'bool'), equals('null'));
    expect(tc.encode(true, null), equals('true'));
    expect(tc.encode(false, null), equals('false'));
    expect(tc.encode(true, 'bool'), equals('true'));
    expect(tc.encode(false, 'bool'), equals('false'));
  });

  test('encode json', () {
    var tc = new TypeConverter();
    expect(tc.encode({"foo": "bar"}, 'json'), equals(' E\'{"foo":"bar"}\' '));
    expect(tc.encode({"foo": "bar"}, null), equals(' E\'{"foo":"bar"}\' '));
    expect(tc.encode({"fo'o": "ba'r"}, 'json'), equals(' E\'{"fo\\\'o":"ba\\\'r"}\' '));
  });

  test('0.2 compatability test.', () {
    var tc = new TypeConverter();
    expect(tc.encode(null, null), equals('null'));
    expect(tc.encode(null, 'string'), equals('null'));
    expect(tc.encode(null, 'number'), equals('null'));
    expect(tc.encode(null, 'foo'), equals('null')); // Should this be an error??

    expect(tc.encode(1, null), equals('1'));
    expect(tc.encode(1, 'number'), equals('1'));
    expect(tc.encode(1, 'string'), equals(" E'1' "));
    expect(tc.encode(1, 'String'), equals(" E'1' "));

    expect(tc.encode(new DateTime.utc(1979,12,20,9), 'date'), equals("'1979-12-20'"));
    expect(tc.encode(new DateTime.utc(1979,12,20,9), 'timestamp'), equals("'1979-12-20T09:00:00.000Z'"));
  });

  
  test('encode BigInt', () {
    var tc = new TypeConverter();
    expect(tc.encode(BigInt.parse('9876543210123456789'), null),
        equals('9876543210123456789'));
    expect(tc.encode(BigInt.parse('-9876543210123456789'), 'bigint'),
        equals('-9876543210123456789'));
    expect(tc.encode(BigInt.two, 'numeric'), equals('2'));
  });

  group('decode json scalar', () {
    const json = 114, jsonb = 3802; //pg type oids (see constants.dart)
    var tc = new TypeConverter();

    test('a json null is a Dart null, not an error', () {
      //a non-null column holding the json scalar `null`, e.g. `"col"->2` on a
      //null element; a SQL NULL never reaches decode (colSize == -1)
      expect(tc.decode('null', json), isNull);
      expect(tc.decode('null', jsonb), isNull);
    });

    test('other scalars decode to values', () {
      expect(tc.decode('5', json), equals(5));
      expect(tc.decode('"abc"', jsonb), equals('abc'));
      expect(tc.decode('{"a": [1, null]}', json), equals({'a': [1, null]}));
    });
  });

  group('decode array (_parseArray)', () {
    //pg type array oids (see constants.dart)
    const text = 1009, varchar = 1015, int4 = 1007, int8 = 1016,
      float8 = 1022, numeric = 1231, bool_ = 1000, date = 1182,
      timestamp = 1115, timestampz = 1185, json = 199, jsonb = 3807,
      money = 791;
    var tc = new TypeConverter();

    test('empty and single', () {
      expect(tc.decode('{}', text), equals([]));
      expect(tc.decode('{a}', text), equals(['a']));
      expect(tc.decode('{"a"}', text), equals(['a']));
      expect(tc.decode('{5}', int4), equals([5]));
    });

    test('unquoted text elements', () {
      expect(tc.decode('{a,b,c}', text), equals(['a', 'b', 'c']));
    });

    test('NULL vs quoted "NULL" vs empty string', () {
      //unquoted NULL is SQL null; quoted is the literal 4-char string
      expect(tc.decode('{NULL}', text), equals([null]));
      expect(tc.decode('{"NULL"}', text), equals(['NULL']));
      expect(tc.decode('{""}', text), equals(['']));
      expect(tc.decode('{NULL,x,NULL}', text), equals([null, 'x', null]));
    });

    test('quoting forced by special chars', () {
      //PG quotes any element containing space, comma, brace, quote or backslash
      expect(tc.decode('{"a b"}', text), equals(['a b']));
      expect(tc.decode('{"a,b"}', text), equals(['a,b']));
      expect(tc.decode('{"{x}"}', text), equals(['{x}']));
      expect(tc.decode('{"a{b}c,d e"}', text), equals(['a{b}c,d e']));
    });

    test('backslash escapes inside quotes', () {
      expect(tc.decode(r'{"a\"b"}', text), equals(['a"b']));   //escaped quote
      expect(tc.decode(r'{"a\\b"}', text), equals([r'a\b']));  //escaped backslash
      expect(tc.decode(r'{"\\"}', text), equals([r'\']));      //lone backslash
      expect(tc.decode(r'{"a\\","b"}', text), equals([r'a\', 'b'])); //escape then delim
    });

    test('mixed quoted / unquoted / null sequence', () {
      expect(tc.decode(r'{plain,"qu\"ot","a,b",NULL,"NULL",""}', text),
          equals(['plain', 'qu"ot', 'a,b', null, 'NULL', '']));
      //quoted and unquoted alternating, ensuring delimiter handling both ways
      expect(tc.decode(r'{"x",y,"z"}', text), equals(['x', 'y', 'z']));
    });

    test('varchar[] behaves like text[]', () {
      expect(tc.decode(r'{"a b",c}', varchar), equals(['a b', 'c']));
    });

    test('integer arrays (int4/int8)', () {
      expect(tc.decode('{1,-2,3,NULL}', int4), equals([1, -2, 3, null]));
      expect(tc.decode('{9223372036854775807,-1}', int8),
          equals([9223372036854775807, -1]));
    });

    test('float and numeric arrays', () {
      expect(tc.decode('{1.5,-2.5,0}', float8), equals([1.5, -2.5, 0.0]));
      expect(tc.decode('{3.14,NULL,2}', numeric), equals([3.14, null, 2.0]));
    });

    test('bool array', () {
      expect(tc.decode('{t,f,NULL,t}', bool_), equals([true, false, null, true]));
    });

    test('date / timestamp / timestamptz arrays', () {
      expect(tc.decode('{2026-07-07,NULL}', date),
          equals([DateTime.parse('2026-07-07T00:00:00Z').toLocal(), null]));
      //timestamps come back quoted (contain a space)
      expect(tc.decode('{"2026-07-07 06:42:35"}', timestamp),
          equals([DateTime.parse('2026-07-07T06:42:35Z').toLocal()]));
      expect(tc.decode('{"2026-07-07 06:42:35+00",NULL}', timestampz),
          equals([DateTime.parse('2026-07-07 06:42:35+00').toLocal(), null]));
    });

    test('money[] left as raw strings', () {
      expect(tc.decode(r'{"$1,000.00","$0.50"}', money),
          equals([r'$1,000.00', r'$0.50']));
    });

    test('json[] / jsonb[] elements decode to values', () {
      //objects -> Map, json string -> its content, numbers/bools -> values,
      //quoted "null" is a json null, unquoted NULL is SQL null
      expect(tc.decode(r'{"{\"a\": 1}","\"abc\"",5,true,NULL,"null"}', json),
          equals([{'a': 1}, 'abc', 5, true, null, null]));
      expect(tc.decode(r'{"[1, 2]","{\"k\": [true, null]}"}', jsonb),
          equals([[1, 2], {'k': [true, null]}]));
      expect(tc.decode('{}', json), equals([]));
    });

    test('unsupported arrays assert (multidim / dimension prefix)', () {
      //multidimensional: outer braces match, nested brace is caught unquoted
      expect(() => tc.decode('{{1,2},{3,4}}', int4), throwsA(isA<AssertionError>()));
      expect(() => tc.decode('{{a},{b}}', text), throwsA(isA<AssertionError>()));
      //non-default lower bound carries a [lo:hi]= dimension prefix
      expect(() => tc.decode('[0:1]={5,6}', int4), throwsA(isA<AssertionError>()));
      //a quoted element containing braces is NOT multidim (no false positive)
      expect(tc.decode(r'{"{x}","{y}"}', text), equals(['{x}', '{y}']));
    });
  });

  //TODO test bytea
}
