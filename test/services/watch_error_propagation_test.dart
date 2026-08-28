import 'dart:convert';
import 'dart:io' as io;

import 'package:drift/drift.dart' hide isNull, isNotNull;
import 'package:drift/native.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:logging/logging.dart';
import 'package:pocketbase_drift/pocketbase_drift.dart';
import 'package:shared_preferences/shared_preferences.dart';

import '../test_data/collections.json.dart';

/// Regression test for a bug where a failed initial fetch inside `onListen`
/// (network down/rejected, no usable cache) was thrown from an unawaited
/// async callback and never reached the stream `watchRecord`/`watchRecords`/
/// `watchRecordState`/`watchRecordsState` return -- it silently vanished
/// (an uncaught async exception) instead of surfacing as a catchable stream
/// error a caller could observe and react to.
///
/// `RequestPolicy.networkOnly` is used because its remote-fetch path never
/// attempts a cache fallback on failure (`_fetchNetworkOnly` rethrows
/// directly) -- the simplest deterministic way to guarantee the initial
/// fetch inside `onListen` throws, regardless of local cache state.
void main() {
  TestWidgetsFlutterBinding.ensureInitialized();
  io.HttpOverrides.global = null;

  const username = 'test@admin.com';
  const password = 'Password123';
  const url = 'http://127.0.0.1:8090';

  late $PocketBase client;
  final collections = [...offlineCollections]
      .map((e) => CollectionModel.fromJson(jsonDecode(jsonEncode(e))))
      .toList();

  setUpAll(() async {
    hierarchicalLoggingEnabled = true;
    Logger.root.level = Level.WARNING;
    Logger.root.onRecord
        // ignore: avoid_print
        .listen((record) => print('${record.level.name}: ${record.message}'));

    SharedPreferences.setMockInitialValues({});
    client = $PocketBase.database(
      url,
      authStore:
          $AuthStore.prefs(await SharedPreferences.getInstance(), 'pb_auth'),
      connection: DatabaseConnection(NativeDatabase.memory()),
      inMemory: true,
    );
    client.logging = true;

    await client.collection('_superusers').authWithPassword(username, password);
    await client.db.setSchema(collections.map((e) => e.toJson()).toList());
  });

  tearDownAll(() {
    client.close();
  });

  test(
      'watchRecords surfaces a networkOnly fetch failure as a stream error '
      'instead of silently vanishing', () async {
    // No matching server-side collection exists for "todo" against this
    // PocketBase instance, so the networkOnly remote fetch inside
    // watchRecords' onListen is guaranteed to fail (404) with no cache
    // fallback attempted.
    final service = await client.$collection('todo');

    final errors = <Object>[];
    final subscription = service
        .watchRecords(requestPolicy: RequestPolicy.networkOnly)
        .listen((_) {}, onError: errors.add);

    // Give the unawaited onListen callback a chance to run and fail.
    await Future<void>.delayed(const Duration(seconds: 2));
    await subscription.cancel();

    expect(errors, isNotEmpty,
        reason: 'the networkOnly fetch failure inside onListen must reach '
            'this stream as a catchable error, not vanish silently');
  });

  test(
      'watchRecordsState surfaces a networkOnly fetch failure as a stream '
      'error instead of silently vanishing', () async {
    final service = await client.$collection('todo');

    final errors = <Object>[];
    final subscription = service
        .watchRecordsState(requestPolicy: RequestPolicy.networkOnly)
        .listen((_) {}, onError: errors.add);

    await Future<void>.delayed(const Duration(seconds: 2));
    await subscription.cancel();

    expect(errors, isNotEmpty,
        reason: 'the networkOnly fetch failure inside onListen must reach '
            'this stream as a catchable error, not vanish silently');
  });

  // watchRecord/watchRecordState (singular) fetch via getOneOrNull, which
  // already swallows every failure internally and returns null rather than
  // rethrowing -- so their added onListen catch block can never actually
  // fire from a fetch failure today, and isn't testable the same way as the
  // plural methods above. What genuinely needed coverage for the singular
  // methods is the addStream -> manual .listen()/controller.add() refactor
  // itself (watchRecord only -- watchRecordState already forwarded
  // manually and was never touched structurally): confirm it still streams
  // live updates correctly instead of silently breaking liveness.
  test('watchRecord still streams live updates after the addStream -> '
      'manual-forwarding refactor', () async {
    final service = await client.$collection('todo');
    await client.db.deleteAll(service.service);

    final created = await service.create(
      body: {'name': 'watch_record_initial'},
      requestPolicy: RequestPolicy.cacheAndNetwork,
    );

    final events = <RecordModel?>[];
    final subscription =
        service.watchRecord(created.id).listen(events.add);
    await Future<void>.delayed(const Duration(milliseconds: 200));

    await service.update(
      created.id,
      body: {'name': 'watch_record_updated'},
      requestPolicy: RequestPolicy.cacheAndNetwork,
    );
    await Future<void>.delayed(const Duration(milliseconds: 200));
    await subscription.cancel();

    expect(events, isNotEmpty);
    expect(events.last?.data['name'], 'watch_record_updated',
        reason: 'watchRecord must still emit live updates through the '
            'manually-forwarded db stream, not just the initial snapshot');
  });
}
