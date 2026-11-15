import 'dart:convert';
import 'dart:typed_data';

/// One complete message: one or more frames, sent and received atomically.
final class Message {
  /// A single-frame message. [routingId] addresses a SERVER or PEER peer;
  /// [group] is the RADIO group.
  Message(Uint8List data, {this.routingId = 0, this.group}) : _first = data, _all = null;

  /// A single-frame UTF-8 message.
  Message.text(String text, {int routingId = 0, String? group})
    : this(utf8.encode(text), routingId: routingId, group: group);

  /// A multipart message. The frames are copied into a new list.
  Message.multipart(Iterable<Uint8List> frames) : this._parts(List.of(frames, growable: false));

  Message._parts(List<Uint8List> frames)
    : _first = frames.isEmpty
          ? throw ArgumentError.value(frames, 'frames', 'must not be empty')
          : frames[0],
      _all = frames.length > 1 ? frames : null,
      routingId = 0,
      group = null;

  Message._(this._first, this._all, this.routingId, this.group);

  // Single-frame messages, the common case, need no frame list.
  final Uint8List _first;
  final List<Uint8List>? _all;

  /// The routing ID of a SERVER or PEER peer, or 0.
  final int routingId;

  /// The RADIO/DISH group, or null.
  final String? group;

  /// The frames, in order.
  List<Uint8List> get frames => _all ?? List.filled(1, _first);

  /// Whether the message has more than one frame.
  bool get isMultipart => _all != null;

  /// The only frame.
  ///
  /// Throws a [StateError] for a multipart message.
  Uint8List get data => _all == null ? _first : throw StateError('message is multipart');

  /// The only frame, decoded as UTF-8.
  ///
  /// Throws a [StateError] for a multipart message.
  String get text => utf8.decode(data);

  @override
  String toString() => isMultipart
      ? 'Message(${_all!.length} frames)'
      : 'Message(${_first.length} bytes${routingId == 0 ? '' : ', routingId $routingId'})';
}

/// A received single-frame message.
Message receivedFrame(Uint8List data, int routingId, String? group) =>
    Message._(data, null, routingId, group);

/// A received multipart message; [frames] has at least two entries.
Message receivedFrames(List<Uint8List> frames) => Message._(frames[0], frames, 0, null);
