import 'package:test/test.dart';
import 'package:postgresql2/postgresql.dart';

void main() {

  final tc = new TypeConverter() as DefaultTypeConverter;

  group('Substitute by id', () {
    test('Substitute A', () {
      var result = substitute("""'@id\\'@id'@id"@id" """,
          {'id': 20}, tc.encodeValue);
      expect(result, equals(  """'@id\\'@id'20"@id" """));
    });

    test('Substitute B', () {
      final dd = r'$d$';
      var result = substitute("""'@id\\'@id' $dd@id$dd @id"@id" """,
          {'id': 20}, tc.encodeValue);
      expect(result, equals(  """'@id\\'@id' $dd@id$dd 20"@id" """));
    });

    test('Substitute C', () {
      var result = substitute("@id@@@id@>@id", {'id': 20}, tc.encodeValue);
      expect(result, equals(  "20@@20@>20"));
    });

    test('Substitute 1', () {
      var result = substitute('@id', {'id': 20}, tc.encodeValue);
      expect(result, equals('20'));
    });

    test('Substitute 2', () {
      var result = substitute('@id ', {'id': 20}, tc.encodeValue);
      expect(result, equals('20 '));
    });

    test('Substitute 3', () {
      var result = substitute(' @id ', {'id': 20}, tc.encodeValue);
      expect(result, equals(' 20 '));
    });

    test('Substitute 4', () {
      var result = substitute('@id@bob', {'id': 20, 'bob': 13}, tc.encodeValue);
      expect(result, equals('2013'));
    });

    test('Substitute 5', () {
      var result = substitute('..@id..', {'id': 20}, tc.encodeValue);
      expect(result, equals('..20..'));
    });

    test('Substitute 6', () {
      var result = substitute('...@id...', {'id': 20}, tc.encodeValue);
      expect(result, equals('...20...'));
    });

    test('Substitute 7', () {
      var result = substitute('...@id.@bob...', {'id': 20, 'bob': 13}, tc.encodeValue);
      expect(result, equals('...20.13...'));
    });

    test('Substitute 8', () {
      var result = substitute('...@id@bob', {'id': 20, 'bob': 13}, tc.encodeValue);
      expect(result, equals('...2013'));
    });

    test('Substitute 9', () {
      var result = substitute('@id@bob...', {'id': 20, 'bob': 13}, tc.encodeValue);
      expect(result, equals('2013...'));
    });

    test('Substitute 10', () {
      var result = substitute('@id:text', {'id': 20, 'bob': 13}, tc.encodeValue);
      expect(result, equals(" E'20' "));
    });

    test('Substitute 11', () {
      var result = substitute('@blah_blah', {'blah_blah': 20}, tc.encodeValue);
      expect(result, equals("20"));
    });

    test('Substitute 12', () {
      var result = substitute('@_blah_blah', {'_blah_blah': 20}, tc.encodeValue);
      expect(result, equals("20"));
    });
    
    test('Substitute 13', () {
      var result = substituteByList('@0 @1', ['foo', 42], tc.encodeValue);
      expect(result, equals(" E'foo'  42"));
    });
  });

  group('Comments', () {
    final enc = (new TypeConverter() as DefaultTypeConverter).encodeValue;

    test('line comment', () {
      expect(substitute('select @x -- a@b\n@x', {'x': 1}, enc),
          equals('select 1 -- a@b\n1'));
      //@y inside a comment must not be looked up (not in the map)
      expect(substitute('@x --@y', {'x': 1}, enc), equals('1 --@y'));
    });

    test('block comment, nested', () {
      expect(substitute('/* @y */@x', {'x': 1}, enc), equals('/* @y */1'));
      expect(substitute('/* /* @y */ @z */@x', {'x': 1}, enc),
          equals('/* /* @y */ @z */1'));
    });

    test('not comments', () {
      expect(substitute('a-b@x', {'x': 1}, enc), equals('a-b1'));
      expect(substitute('a/b @x', {'x': 1}, enc), equals('a/b 1'));
    });
  });

  group('Dollar quotes', () {
    final enc = (new TypeConverter() as DefaultTypeConverter).encodeValue;

    test('untagged', () {
      expect(substitute(r'$$ @y $$@x', {'x': 1}, enc), equals(r'$$ @y $$1'));
      //a bare $ in the body must not close the quote
      expect(substitute(r'$$ $5 @y $$@x', {'x': 1}, enc),
          equals(r'$$ $5 @y $$1'));
    });

    test('tagged', () {
      expect(substitute(r'$t$@y$t$@x', {'x': 1}, enc), equals(r'$t$@y$t$1'));
      //near-miss terminator ($ta$) must not close $tag$
      expect(substitute(r'$tag$a$ta$@y$tag$@x', {'x': 1}, enc),
          equals(r'$tag$a$ta$@y$tag$1'));
    });

    test('non-ASCII tag', () {
      expect(substitute(r'$déjà$ @y $déjà$@x', {'x': 1}, enc),
          equals(r'$déjà$ @y $déjà$1'));
    });

    test('not dollar quotes', () {
      //$1 is a positional param, not a tag (digit can't start a tag)
      expect(substitute(r'select $1, @x', {'x': 1}, enc),
          equals(r'select $1, 1'));
    });

    test('unterminated', () {
      expect(substitute(r'$$ @y', {'x': 1}, enc), equals(r'$$ @y'));
    });

//    test('Substitute 13', () {
//      var result = substitute('@apos', {'apos': "'"});
//      //expect(result, equals("E'''"));
//      //print('oi');
//      print(result);
//    });
  });

}