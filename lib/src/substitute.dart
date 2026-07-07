library postgresql.substitute;

import 'dart:collection';
import 'package:charcode/ascii.dart';
import 'package:rikulo_commons/util.dart';

const int _TOKEN_TEXT = 1;
const int _TOKEN_IDENT = 3;

class _Token {
  _Token(this.type, this.value, [this.typeName]);
  final int type;
  final String value;
  final String? typeName;

  @override
  String toString() => '${['?', 'Text', 'At', 'Ident'][type]} "$value" "$typeName"';
}

typedef String _ValueEncoder(String identifier, String? type);

bool isIdentifier(int charCode)
  => (charCode >= $a && charCode <= $z)
  || (charCode >= $A && charCode <= $Z)
  || (charCode >= $0 && charCode <= $9)
  || (charCode == $underscore);

bool isDigit(int charCode) => (charCode >= $0 && charCode <= $9);

class ParseException {
  ParseException(this.message, [this.source, this.index]);
  final String message;
  final String? source;
  final int? index;

  @override
  String toString() => (source == null || index == null) ? message
      : '$message At character: $index, in source "$source"';
}

String substitute(String source, Map? values,
    String encodeValue(value, String? type))
=> _substitute(source, values == null ? _nullValueEncoder:
    _createMapValueEncoder(values, encodeValue), encodeValue);

String substituteByList(String source, List? values,
    String encodeValue(value, String? type))
=> _substitute(source, values == null ? _nullValueEncoder:
    _createListValueEncoder(values, encodeValue), encodeValue);

String _substitute(String source, _ValueEncoder valueEncoder,
    String encodeValue(value, String? type)) {
  final buf = StringBuffer(),
    s = new _Scanner(source),
    cache = HashMap();

  while (s.hasMore()) {
    var t = s.read()!;
    if (t.type == _TOKEN_IDENT) {
      final id = t.value,
        typeName = t.typeName,
        key = Pair(id, typeName);
      buf.write(cache[key] ?? (cache[key] = valueEncoder(id, typeName)));
    } else {
      buf.write(t.value);
    }
  }

  return buf.toString();
}

String _nullValueEncoder(value, String? type)
=> throw ParseException('Template contains a parameter, but no values were passed.');

_ValueEncoder _createListValueEncoder(List list,
    String encodeValue(value, String? type))
  => (String identifier, String? type) {
  int i = int.tryParse(identifier) ??
      (throw ParseException('Expected integer parameter.'));

  if (i < 0 || i >= list.length)
    throw ParseException('Substitution token out of range.');

  return encodeValue(list[i], type);
};

_ValueEncoder _createMapValueEncoder(Map map,
    String encodeValue(value, String? type))
  => (String identifier, String? type) {
  final val = map[identifier];
  if (val == null && !map.containsKey(identifier))
    throw ParseException("Substitution token not passed: $identifier.");

  return encodeValue(val, type);
};

class _Scanner {
  _Scanner(String source)
      : //_source = source,
        _r = new _CharReader(source) {

    if (_r.hasMore())
      _t = _read();
  }

  //final String _source;
  final _CharReader _r;
  _Token? _t;

  bool hasMore() => _t != null;

  _Token? peek() => _t;

  _Token? read() {
    var t = _t;
    _t = _r.hasMore() ? _read() : null;
    return t;
  }

  _Token _read() {

    assert(_r.hasMore());

    // '@@', '@ident', or '@ident:type'
    if (_r.peek() == $at) {
      _r.read();

      if (!_r.hasMore())
        throw ParseException('Unexpected end of input.');

      // '@@' or '@>' operator and '<@ '
      if (!isIdentifier(_r.peek())) {
        final s = String.fromCharCode(_r.read());
        return new _Token(_TOKEN_TEXT, '@$s');
      }

      // Identifier
      var ident = _r.readWhile(isIdentifier);

      // Optional type modifier
      var type;
      if (_r.peek() == $colon) {
        _r.read();
        type = _r.readWhile(isIdentifier);
      }
      return new _Token(_TOKEN_IDENT, ident, type);
    }

    // Read plain text
    var text = _readText();
    return new _Token(_TOKEN_TEXT, text);
  }

  String _readText() {
    final r = _r;
    final start = r.index;
    int? esc; //quote char when inside '...' or "..."
    bool escBackslash = false; //E'...': backslash escapes the next char
    bool backslash = false;

    while (r.hasMore()) {
      final c = r.peek();

      if (backslash) {
        backslash = false;

      } else if (esc != null) {
        if (escBackslash && c == $backslash) backslash = true;
        else if (c == esc) esc = null;

      } else if (c == $at) {
        break; //found

      } else if (c == $backslash) {
        backslash = true;

      } else if (c == $single_quote || c == $quot) {
        esc = c;
        //PG (standard_conforming_strings): backslash escapes only in E'...';
        //literal in '...' and "..." ('' still works: scanned as close+reopen)
        final p = r.peekBehind();
        escBackslash = c == $single_quote && (p == $e || p == $E)
            && !isIdentifier(r.peekBehind(2)); //`else'x\'` is not an E-string

      } else if (c == $dollar) { //dollar quote? (`$tag$...$tag$`; not `$1`)
        r.read();
        final tag = _readDollarTag();
        if (tag != null) _skipDollarQuoted(tag);
        continue;

      } else if (c == $dash) { //`--` line comment?
        r.read();
        if (r.peek() == $dash) r.skipPast('\n');
        continue;

      } else if (c == $slash) { //`/*...*/` block comment (nestable)?
        r.read();
        if (r.peek() == $asterisk) _skipBlockComment();
        continue;
      }

      r.read();
    }
    return r.substringFrom(start);
  }

  /// Reads `tag$` after the opening `$`; null if not a dollar quote (e.g. `$1`).
  String? _readDollarTag() {
    final r = _r;
    final start = r.index;
    for (bool first = true;; first = false) {
      if (!r.hasMore()) return null;
      final c = r.peek();
      if (c == $dollar) {
        final tag = r.substringFrom(start);
        r.read();
        return tag;
      }
      if (first ? !_isTagStart(c) : !_isTagChar(c)) return null;
      r.read();
    }
  }

  /// Consumes through the matching `$tag$` terminator.
  void _skipDollarQuoted(String tag) => _r.skipPast('\$$tag\$');

  void _skipBlockComment() {
    final r = _r;
    r.read(); //the '*'
    for (int depth = 1; r.hasMore();) {
      final c = r.read();
      if (c == $asterisk && r.peek() == $slash) {
        r.read();
        if (--depth == 0) return;
      } else if (c == $slash && r.peek() == $asterisk) {
        r.read();
        ++depth;
      }
    }
  }
}

//PG's lexer treats any high-bit char as a letter in dollar-quote tags
bool _isTagStart(int c)
  => (c >= $a && c <= $z)
  || (c >= $A && c <= $Z)
  || c == $underscore || c >= 0x80;
bool _isTagChar(int c) => _isTagStart(c) || isDigit(c);

class _CharReader {
  _CharReader(String source)
      : _source = source, _codes = source.codeUnits;

  final String _source;
  final List<int> _codes;
  int _i = 0;

  bool hasMore() => _i < _codes.length;

  int get index => _i;

  int read() => hasMore() ? _codes[_i++]: 0;
  int peek() => hasMore() ? _codes[_i]: 0;

  /// The [n]-th char before the current position, or 0 if out of range.
  int peekBehind([int n = 1]) => _i >= n ? _codes[_i - n]: 0;

  String substringFrom(int start) => _source.substring(start, _i);

  /// Advances past the first occurrence of [s], or to the end if absent.
  void skipPast(String s) {
    final i = _source.indexOf(s, _i);
    _i = i < 0 ? _codes.length: i + s.length;
  }

  String readWhile(bool test(int charCode)) {
    if (!hasMore())
      throw ParseException('Unexpected end of input.', _source, _i);

    int start = _i;

    while (hasMore() && test(peek())) {
      read();
    }

    return String.fromCharCodes(_codes.sublist(start, _i));
  }
}
