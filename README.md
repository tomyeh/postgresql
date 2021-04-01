# PostgreSQL database driver for Dart

> A fork of [Greg's PostgreSQL driver](https://github.com/xxgreg/postgresql).

**Differences**

* Array type with single dimension supported

* The `@@` operators supported.

* `DefaultTypeConverter.encodeValueDefault()` assume all unknown types as JSON.

* `Pool.connect()` throws an exception immediately if failed to connect to database.

* More options to control the pool, such as `limitTimeout` and `limitConnections`

## Basic usage

* [API Reference](http://www.dartdocs.org/documentation/postgresql2/1.0.0-beta)

### Obtaining a connection

```dart
var uri = 'postgres://username:password@localhost:5432/database';
connect(uri).then((conn) {
	// ...
});
```

### SSL connections

Set the sslmode to require by appending this to the connection uri. This driver only supports sslmode=require, if sslmode is ommitted the driver will always connect without using SSL.

```dart
var uri = 'postgres://username:password@localhost:5432/database?sslmode=require';
connect(uri).then((conn) {
	// ...
});
```

### Querying

```dart
conn.query('select color from crayons').toList().then((rows) {
	for (var row in rows) {
		print(row.color); // Refer to columns by name,
		print(row[0]);    // Or by column index.
	}
});
```

### Executing

```dart
conn.execute("update crayons set color = 'pink'").then((rowsAffected) {
	print(rowsAffected);
});
```

### Query Parameters

Query parameters can be provided using a map. Strings will be escaped to prevent SQL injection vulnerabilities.

```dart
conn.query('select color from crayons where id = @id', {'id': 5})
  .toList()
	.then((result) { print(result); });

conn.execute('insert into crayons values (@id, @color)',
             {'id': 1, 'color': 'pink'})
	.then((_) { print('done.'); });
```

### Closing the connection

You must remember to call Connection.close() when you're done. This won't be
done automatically for you.

### Conversion of Postgresql datatypes.

Below is the mapping from Postgresql types to Dart types. All types which do not have an explicit mapping will be returned as a String in Postgresql's standard text format. This means that it is still possible to handle all types, as you can parse the string yourself.

```
     Postgresql type                 Dart type
	boolean                         bool
	int2, int4, int8                int
	float4, float8                  double
	numeric                         String
	timestamp, timestamptz, date    Datetime
	json, jsonb                     Map/List
	All other types                 String
```

### Mapping the results of a query to an object

```dart
class Crayon {
	String color;
	int length;
}

conn.query('select color, length from crayons')
	.map((row) => new Crayon()
	                     ..color = row.color
	                     ..length = row.length)
	.toList()
	.then((List<Crayon> crayons) {
		for (var c in crayons) {
			print(c is Crayon);
			print(c.color);
			print(c.length);
		}
	});
```

Or for an immutable object:

```dart
class ImmutableCrayon {
	ImmutableCrayon(this.color, this.length);
	final String color;
	final int length;
}

conn.query('select color, length from crayons')
  .map((row) => new ImmutableCrayon(row.color, row.length))
     .toList()
	.then((List<ImmutableCrayon> crayons) {
		for (var c in crayons) {
			print(c is ImmutableCrayon);
			print(c.color);
			print(c.length);
		}
	});
```

### Query queueing

Queries are queued and executed in the order in which they were queued.

So if you're not concerned about handling errors, you can write code like this:

```dart
conn.execute("create table crayons (color text, length int)");
conn.execute("insert into crayons values ('pink', 5)");
conn.query("select color from crayons").single.then((crayon) {
	print(crayon.color); // prints 'pink'
});
```

### Query streaming

Connection.query() returns a Stream of results. You can use each row as soon as
it is received, or you can wait till they all arrive by calling Stream.toList().

### Connection pooling

In server applications, a connection pool can be used to avoid the overhead of obtaining a connection for each request.

```dart
import 'package:postgresql2/pool.dart';

main() {
  var uri = 'postgres://username:password@localhost:5432/database';
  var pool = new Pool(uri, minConnections: 2, maxConnections: 5);
  pool.messages.listen(print);
  pool.start().then((_) {
    print('Min connections established.');
    pool.connect().then((conn) { // Obtain connection from pool
      conn.query("select 'oi';")
        .toList()
        .then(print)
        .then((_) => conn.close()) // Return connection to pool
        .catchError((err) => print('Query error: $err'));
    });
  });
}
```

### Example program

Add postgresql to your pubspec.yaml file, and run pub install.

```
name: postgresql_example
dependencies:
  postgresql2: any
```

```dart
import 'package:postgresql2/postgresql.dart';

void main() {
  var uri = 'postgres://testdb:password@localhost:5432/testdb';
  var sql = "select 'oi'"; 
  connect(uri).then((conn) {
    conn.query(sql).toList()
    	.then((result) {
    		print('result: $result');
    	})
    	.whenComplete(() {
    		conn.close();
    	});
  });
}
```

## Testing

To run the unit tests you will need to create a database, and edit
'test/config.yaml' accordingly.

### Creating a database for testing

Change to the postgres user and run the administration commands.
```bash
sudo su postgres
createuser --pwprompt testdb
  Enter password for new role: password
  Enter it again: password
  Shall the new role be a superuser? (y/n) n
  Shall the new role be allowed to create databases? (y/n) n
  Shall the new role be allowed to create more new roles? (y/n) n
createdb --owner testdb testdb
exit
```

Check that it worked by logging in.
```bash
psql -h localhost -U testdb -W
```

Enter "\q" to quit from the psql console.

## Notes

### Substitution of Array types

Like others, you can specify the `array` type in a substitution. For example,

```insert into foo values(@name, @tags:array)```

However, if the list can be empty, you have to specify the type explicitly since PostgreSQL cannot determine type of empty array. For example,

```insert into foo values(@name, @tags:text_array)```

It is equivalent to the following:

```insert into foo values(@name, @tags:array::text[])```

## License

BSD

## Utilities

* Transaction management: https://github.com/rikulo/access
* Entity management: https://github.com/rikulo/entity

## Links

* https://www.postgresql.org/docs/9.2/static/index.html
* https://www.dartlang.org/

## Who Uses

* [Quire](https://quire.io) - a simple, collaborative, multi-level task management tool.
* [Keikai](https://keikai.io) - a sophisticated spreadsheet for big data
