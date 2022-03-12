import 'dart:ui';
import 'dart:isolate';

import 'package:hive/hive.dart';
import 'package:hive/src/binary/binary_reader_impl.dart';
import 'package:hive/src/binary/binary_writer_impl.dart';
import 'package:hive/src/registry/type_registry_impl.dart';
import 'package:hive/src/adapters/date_time_adapter.dart';
import 'package:hive/src/adapters/big_int_adapter.dart';
import 'dart:typed_data';

/// Handles communication to Hive, via Isolate if necessary so Hive can be used by multiple Isolates.
///
/// IsolateNameServer is used to give a communication port to any other Isolate that wants to use Hive.
/// Is is a good idea to give [identifier] an identifier unique to your application, as I do not know if other apps will be able to access IsolateNameServer.
/// I think each app creates a new VM service but who knows. They won't have the same Hive.homePath anyway so all in all we should be good.
/// You need to provide a [initAdapters] function, which be called twice in each Isolate.
/// The Hive Isolate will be spawned if [canBeTheIsolate] is set to false and it doesn't exists yet when calling any HiveIsolateBox function.
/// If [canBeTheIsolate] is true, then the Hive Isolate will be the calling Isolate.
/// Simply put, leave as is and your main app Isolate will most likely be the Hive Isolate, resulting in the same performances as using Hive normally.
class HiveIsolateInterface {

  /// Boxes opened.
  static final Map<String,dynamic> _boxes = {};

  /// The port used to communicate with the Hive Isolate. Will reference its own Isolate if [_iAmTheIsolate] is true.
  static SendPort? _port;

  /// Unique identifier used to get the port via IsolateNameServer
  static String identifier = 'HiveInterface';

  /// Delay of response of the alive check before asking something of the Hive Isolate. Not used if [_iAmTheIsolate] is true.
  static int isAliveDelayInMilliseconds = 100;

  /// Is true if the current Isolate is the Hive Isolate
  static bool _iAmTheIsolate = false;

  /// Decides if the current Isolate can be the Hive Isolate. Used to force the creation of a specific Isolate only for Hive.
  static bool canBeTheIsolate = true;

  /// An Isolate cannot use path_provider. You need to set this manually. I advice saving the app's path from the main function via shared_preferences.
  static String appDirPath = '';

  /// The function initializing the adapters. Will be passed to the HiveIsolate on spawning and called immediatly.
  static void Function(TypeRegistry reg) initAdapters = (reg) => null;

  /// Used to serialise data for transfert the result of the query to the Hive Isolate.
  static TypeRegistryImpl _typeImpl = TypeRegistryImpl();

  /// Valid be true once the [_registerAdapters] Function is called, for it can only be called once per Isolate.
  static bool adaptersRegistered = false;


  /// Checks if a Hive Isolate exists. If [_iAmTheIsolate] is true, returns immediately.
  /// If no Hive Isolate exists, creates one. If it exists, checks that it's alive with timeout of [isAliveDelayInMilliseconds]
  static Future<void> _initIsolate() async {

    // Verify adapters are registered
    _registerAdapters();

    // If the interface is in this Isolate, all is well.
    if(_iAmTheIsolate) return;

    // We reload the port every time so that if the isolate gets killed, we can spawn it again without causing problems
    _port = IsolateNameServer.lookupPortByName(identifier);

    // Verify that Isolate is still responding by asking it to return true. It it responds then all is well.
    // This is faster then waiting for a bigger maxRequestDelay in case the Isolate died.
    // You can set the isAliveDelay to 0 to bypass this.
    if(_port != null) {
      ReceivePort response = ReceivePort();
      _port!.send([response.sendPort, _isAlive]);
      if(await response.first.timeout(Duration(milliseconds: isAliveDelayInMilliseconds), onTimeout: () => false) == false) {
        // Delete the port and go to spawn a new Isolate.
        _port = null;
        IsolateNameServer.removePortNameMapping(identifier);
      }
    }

    // There is no interface currently online, therefore I am the interface.
    if(_port == null) {
      ReceivePort tempPort = ReceivePort();
      if(canBeTheIsolate) {
       _isolateEntryPoint([tempPort.sendPort, appDirPath, initAdapters]);
      } else {
        await Isolate.spawn(_isolateEntryPoint, [tempPort.sendPort, appDirPath, initAdapters]);
      }
      _port = await tempPort.first;
      IsolateNameServer.registerPortWithName(_port!, identifier);
    }
  }

  /// Used by [_initIsolate] to verify the port given by IsolateNameServer still points to a Isolate that is alive.
  /// Needs to be a top-level function so the Isolate to access it.
  static bool _isAlive() => true;

  /// Isolate within which hive will function. You need to call a function from it which initialize hive & adapters (see HiveInterface.initHive).
  /// Will put [_iAmTheIsolate] to true immediately.
  /// [args] is [SendPort, homePath]
  static void _isolateEntryPoint(List<dynamic> args) {
    _iAmTheIsolate = true;
    ReceivePort receivePort = ReceivePort();
    args[0].send(receivePort.sendPort);

    // Path to home of Hive
    Hive.init(args[1]);

    // Register adapters
    initAdapters = args[2];
    _registerAdapters();

    receivePort.listen((message) async {
      var res = await _doJob(message);
      if(message[1] == null) message[0].send(null);
      if(message[1] == true) message[0].send(_encodeData(res));
       message[0].send(res);
    });
  }

  /// Registers the internal adapters plus those given by [getAdapters].
  static void _registerAdapters() {
    if(adaptersRegistered) return;
    _typeImpl.registerAdapter(DateTimeWithTimezoneAdapter(), internal: true);
    _typeImpl.registerAdapter(DateTimeAdapter<DateTimeWithoutTZ>(), internal: true);
    _typeImpl.registerAdapter(BigIntAdapter(), internal: true);
    initAdapters(_typeImpl);
    initAdapters(Hive);
    adaptersRegistered = true;
  }

  /// Encodes an Object or a list of Objects to Uint8List using Hive Adapters.
  static dynamic _encodeData(data) {
    if(data is List) {
      return data.map<Uint8List>((e) => _encodeOneData(e)).toList();
    } else {
      return _encodeOneData(data);
    }
  }

  /// Encodes an Object to Uint8List using BinaryWriterImpl.
  static Uint8List _encodeOneData(value) {
    BinaryWriterImpl writerImpl = BinaryWriterImpl(_typeImpl);
    writerImpl.write(value);
    return writerImpl.toBytes();
  }

  /// Execute a request in the Hive Isolate.
  static dynamic _doJob(message) async {
    switch(message.length) {
      case 3:
        return await message[2]();
      case 4:
        return await message[2](message[3]);
      case 5:
        return await message[2](message[3],message[4]);
      case 6:
        return await message[2](message[3],message[4],message[5]);
      default:
        return null;
    }
  }

  /// Execute a request to the Hive Isolate.
  static Future<dynamic> _doIt<T>(List<dynamic> args) async {
    await _initIsolate();
    if(_iAmTheIsolate) {
      return await _doJob([null,...args]);
    } else {
      ReceivePort response = ReceivePort();
      args.insert(0, response.sendPort);
      _port!.send(args);
      var result = await response.first;
      if(args[1] == true && result is List) return result.map((e) => _decodeOneData<T>(e)).toList();
      if(args[1] == true) return _decodeOneData<T>(result);
      return result;
    }
  }

  /// Decodes an Object to Uint8List using BinaryReaderImpl.
  static T _decodeOneData<T>(data) {
    BinaryReaderImpl readerImpl = BinaryReaderImpl(data, _typeImpl);
    return readerImpl.read();
  }

  /// Returns a box according to the type and name. Opens it if necessary.
  static Future<Box<T>> _getBox<T>(String boxName) async {
    if(_boxes.containsKey(boxName)) return _boxes[boxName];
    _boxes[boxName] = await Hive.openBox<T>(boxName);
    return _boxes[boxName];
  }

  // Access functions called from the box
  static Future<void> _put<T>(String boxName, String key, T value) async => (await _getBox<T>(boxName)).put(key, value);
  static Future<T?> _get<T>(String boxName, String key) async => (await _getBox<T>(boxName)).get(key);
  static Future<List<T>> _getAll<T>(String boxName) async => (await _getBox<T>(boxName)).values.toList();
  static Future<List<T>> _getAllWhere<T>(String boxName, bool Function(T) include) async => (await _getBox<T>(boxName)).values.where((element) => include(element)).toList();
  static Future<bool> _exists<T>(String boxName, String key) async => (await _getBox<T>(boxName)).containsKey(key);
  static Future<void> _remove<T>(String boxName, String key) async => (await _getBox<T>(boxName)).delete(key);
  static Future<void> _clear<T>(String boxName) async => (await _getBox<T>(boxName)).clear();
  static Future<void> _addAll<T>(String boxName, List<T> values) async => (await _getBox<T>(boxName)).addAll(values);
  static Future<int> _count<T>(String boxName) async => (await _getBox<T>(boxName)).length;

  static Future<bool> _toggleValue<T>(String key, T value, String boxName) async {
    Box<T> box = await _getBox<T>(boxName);
    if(box.containsKey(key)) {
      await box.delete(key);
      return false;
    } else {
      await box.put(key,value);
      return true;
    }
  }

  static Future<void> _putAll<T>(List<T> values, Function(T) getKey, String boxName) async {
    Box<T> box = await _getBox<T>(boxName);
    for(T value in values) await box.put(getKey(value), value);
  }

}

/// Rough equivalent to a Hive Box, but will call the HiveIsolateInterface.
class HiveIsolateBox<T> {

  final String boxName;
  final bool usesCustomAdapter;

  HiveIsolateBox(this.boxName, [this.usesCustomAdapter = true]);

  Future<void> put(String key, T value) async => await HiveIsolateInterface._doIt<T>([null, HiveIsolateInterface._put<T>, boxName, key, value]);
  Future<T?> get(String key) async => await HiveIsolateInterface._doIt<T>([usesCustomAdapter, HiveIsolateInterface._get<T>, boxName, key]);
  Future<List<T>> getAll() async => await HiveIsolateInterface._doIt<T>([usesCustomAdapter, HiveIsolateInterface._getAll<T>, boxName]);
  /// [include] needs to be top-level or static function. Use this only went transporting ungodly amounts of data.
  Future<List<T>> getAllWhere(bool Function(T) include) async => await HiveIsolateInterface._doIt<T>([usesCustomAdapter, HiveIsolateInterface._getAllWhere<T>, boxName, include]);
  Future<bool> exists(String key) async => await HiveIsolateInterface._doIt<T>([false, HiveIsolateInterface._exists<T>, boxName, key]);
  Future<bool> toggle(String key, T value) async => await HiveIsolateInterface._doIt<T>([false, HiveIsolateInterface._toggleValue<T>, boxName, key, value]);
  Future<void> remove(String key) async => await HiveIsolateInterface._doIt<T>([null, HiveIsolateInterface._remove<T>, boxName, key]);
  /// [include] needs to be top-level or static function. Use this only went transporting ungodly amounts of data.
  Future<void> putAll(List<T> values, Function(T) getKey) async => await HiveIsolateInterface._doIt<T>([null, HiveIsolateInterface._putAll<T>, boxName, values, getKey]);
  Future<void> clear() async => await HiveIsolateInterface._doIt<T>([null, HiveIsolateInterface._clear<T>, boxName]);
  Future<void> addAll(List<T> values) async => await HiveIsolateInterface._doIt<T>([null, HiveIsolateInterface._addAll<T>, boxName, values]);
  Future<int> count() async => await HiveIsolateInterface._doIt<T>([false, HiveIsolateInterface._count<T>, boxName]);

}
