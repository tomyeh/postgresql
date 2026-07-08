import 'package:test/test.dart';
import 'package:postgresql2/src/postgresql_impl/postgresql_impl.dart'
    show ConnectionImpl;

/// Pinned vectors: libpq pg_md5_encrypt(password, user, salt) —
/// `'md5' + md5_hex(md5_hex(password + user) ++ salt)`, all byte-level.
void main() {
  test('md5 password hash matches libpq', () {
    expect(ConnectionImpl.md5PasswordHash('secret', 'bob', [1, 2, 3, 4]),
        'md5f21dfe33ff3a9e03dbc3e008251fe5cc');
    //salt bytes >= 0x80 are appended raw, not UTF-8 encoded
    expect(ConnectionImpl.md5PasswordHash('secret', 'bob', [0xAB, 2, 3, 0xFF]),
        'md5aacb2ecfeea4f02d232461ba35ecb94c');
    //non-ASCII credentials hash as UTF-8 bytes
    expect(ConnectionImpl.md5PasswordHash('pässword', 'bob', [1, 2, 3, 4]),
        'md58801254ef8b74cfd46729bbd9d0a7bbd');
    expect(ConnectionImpl.md5PasswordHash('pässword', 'bob', [0xAB, 2, 3, 0xFF]),
        'md5a3d168e5f1dc70945236b20a60812232');
  });
}
