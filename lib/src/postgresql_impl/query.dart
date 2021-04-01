part of postgresql.impl;

class _Query {
  int _state = _QUEUED;
  final String sql;
  final StreamController<_Row> _controller = new StreamController<_Row>();
  int _commandIndex = 0;
  int? _columnCount;
  List<_Column>? _columns;
  List<dynamic>? _rowData;
  int? _rowsAffected;

  List<String>? _columnNames;
  Map<Symbol, int>? _columnIndex;

  _Query(this.sql);

  Stream<dynamic> get stream => _controller.stream;

  void addRowDescription() {
    if (_state == _QUEUED)
      _state = _STREAMING;

    final columnNames = _columnNames = _columns!.map((c) => c.name).toList(),
      columnIndex = _columnIndex = new Map<Symbol, int>();
    for (var i = 0; i < columnNames.length; i++) {
      var name = columnNames[i];
      if (_reIdent.hasMatch(name))
        columnIndex[new Symbol(name)] = i;
    }
  }
  static final _reIdent = new RegExp(r'^[a-zA-Z][a-zA-Z0-9_]*$');

  void addRow() {
    var row = new _Row(_columnNames!, _rowData!, _columnIndex!, _columns!);
    _rowData = null;
    _controller.add(row);
  }

  void addError(Object err) {
    _controller.addError(err);
    // stream will be closed once the ready for query message is received.
  }

  void close() {
    _controller.close();
    _state = _DONE;
  }
}

//TODO rename to field, as it may not be a column.
class _Column implements Column {
  @override
  final int index;
  @override
  final String name;

  //TODO figure out what to name these.
  // Perhaps just use libpq names as they will be documented in existing code
  // examples. It may not be neccesary to store all of this info.
  @override
  final int fieldId;
  @override
  final int tableColNo;
  @override
  final int fieldType;
  @override
  final int dataSize;
  @override
  final int typeModifier;
  @override
  final int formatCode;

  @override
  bool get isBinary => formatCode == 1;

  _Column(this.index, this.name, this.fieldId, this.tableColNo, this.fieldType, this.dataSize, this.typeModifier, this.formatCode);

  @override
  String toString() => 'Column: index: $index, name: $name, fieldId: $fieldId, tableColNo: $tableColNo, fieldType: $fieldType, dataSize: $dataSize, typeModifier: $typeModifier, formatCode: $formatCode.';
}

class _Row implements Row {
  _Row(this._columnNames, this._columnValues, this._index, this._columns) {
    assert(this._columnNames.length == this._columnValues.length);
  }

  // Map column name to column index
  final Map<Symbol, int> _index;
  final List<String> _columnNames;
  final List _columnValues;
  final List<Column> _columns;

  @override
  operator[] (int i) => _columnValues[i];

  @override
  void forEach(void f(String columnName, columnValue)) {
    assert(_columnValues.length == _columnNames.length);
    for (int i = 0; i < _columnValues.length; i++) {
      f(_columnNames[i], _columnValues[i]);
    }
  }

  @override
  noSuchMethod(Invocation invocation) {
    var name = invocation.memberName;
    if (invocation.isGetter) {
      var i = _index[name];
      if (i != null)
        return _columnValues[i];
    }
    super.noSuchMethod(invocation);
  }

  @override
  String toString() => _columnValues.toString();

  @override
  List toList() => new UnmodifiableListView(_columnValues);

  @override
  Map<String, dynamic> toMap() => new Map<String, dynamic>.fromIterables(_columnNames, _columnValues);

  @override
  List<Column> getColumns() => new UnmodifiableListView<Column>(_columns);
}


