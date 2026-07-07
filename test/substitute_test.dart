import 'package:test/test.dart';
import 'package:postgresql2/postgresql.dart';

void main() {

  final tc = new TypeConverter() as DefaultTypeConverter;

  group('Substitute by id', () {
    test('Substitute A', () {
      //\' holds the string open only in E'...' (see the Strings group)
      var result = substitute("""E'@id\\'@id'@id"@id" """,
          {'id': 20}, tc.encodeValue);
      expect(result, equals(  """E'@id\\'@id'20"@id" """));
    });

    test('Substitute B', () {
      final dd = r'$d$';
      var result = substitute("""E'@id\\'@id' $dd@id$dd @id"@id" """,
          {'id': 20}, tc.encodeValue);
      expect(result, equals(  """E'@id\\'@id' $dd@id$dd 20"@id" """));
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

    test('trailing line comment (no newline)', () {
      expect(substitute('@x --', {'x': 1}, enc), equals('1 --'));
      expect(substitute('@x --@y', {'x': 1}, enc), equals('1 --@y'));
    });

    test('unterminated block comment', () {
      expect(substitute('/* @y', {'x': 1}, enc), equals('/* @y'));
      expect(substitute('@x /* /* @y */', {'x': 1}, enc),
          equals('1 /* /* @y */')); //depth never reaches 0
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

    test('two dollar quotes in a row', () {
      expect(substitute(r'$$ @a $$ @x $$ @b $$ @y', {'x': 1, 'y': 2}, enc),
          equals(r'$$ @a $$ 1 $$ @b $$ 2'));
    });
  });

  group('Strings', () {
    final enc = (new TypeConverter() as DefaultTypeConverter).encodeValue;

    test("backslash is literal in '...' (standard_conforming_strings)", () {
      //the string closes at the quote right after the backslash
      expect(substitute(r"'a\' @x", {'x': 1}, enc), equals(r"'a\' 1"));
      expect(substitute(r"'\d+' @x", {'x': 1}, enc), equals(r"'\d+' 1"));
    });

    test("backslash escapes in E'...'", () {
      //\' does not close: @y stays inside the string, @x follows it
      expect(substitute(r"E'a\' @y' @x", {'x': 1}, enc),
          equals(r"E'a\' @y' 1"));
      expect(substitute(r"e'a\' @y' @x", {'x': 1}, enc),
          equals(r"e'a\' @y' 1"));
    });

    test("identifier ending in e is not an E-string", () {
      expect(substitute(r"else'a\' @x", {'x': 1}, enc),
          equals(r"else'a\' 1"));
    });

    test('backslash is literal in "..."', () {
      expect(substitute(r'"a\" @x', {'x': 1}, enc), equals(r'"a\" 1'));
    });

    test("doubled quote still spans", () {
      //'' scans as close+reopen: @y stays inside, net effect is one string
      expect(substitute(r"'it''s @y' @x", {'x': 1}, enc),
          equals(r"'it''s @y' 1"));
    });

    test("E-string after punctuation", () {
      //E preceded by a non-identifier char is still an E-string prefix
      expect(substitute(r"(E'a\' @y')@x", {'x': 1}, enc),
          equals(r"(E'a\' @y')1"));
      expect(substitute(r"a=E'\' @y' @x", {'x': 1}, enc),
          equals(r"a=E'\' @y' 1"));
    });

    test("empty and escaped-backslash E-strings", () {
      expect(substitute(r"E'' @x", {'x': 1}, enc), equals(r"E'' 1"));
      //\\ consumes both: the following quote closes
      expect(substitute(r"E'\\' @x", {'x': 1}, enc), equals(r"E'\\' 1"));
    });

    test("backslash is literal in U&'...'", () {
      //U&'s escape char introduces hex digits, it never escapes the quote
      expect(substitute(r"U&'d\' @x", {'x': 1}, enc), equals(r"U&'d\' 1"));
    });

    test('adjacent strings', () {
      //first string closes at \'; the second swallows @y
      expect(substitute(r"'a\' '@y' @x", {'x': 1}, enc),
          equals(r"'a\' '@y' 1"));
    });

//    test('Substitute 13', () {
//      var result = substitute('@apos', {'apos': "'"});
//      //expect(result, equals("E'''"));
//      //print('oi');
//      print(result);
//    });
  });

}