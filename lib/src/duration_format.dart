library postgresql.duration_format;

int parseInt(String s, {required int onError(String s)})
=> int.tryParse(s) ?? onError(s);

class DurationFormat {

  DurationFormat()
    : _approx = false, _threshold = 0;
  
  DurationFormat.approximate([int threshold = 5])
    : _approx = true, _threshold = threshold;
  
  final bool _approx;
  final int _threshold;
    
  Duration parse(String s, {onError(String s)?}) {
    ex() => FormatException('Cannot parse string as duration: "$s".');
    
    parsePrefix(s, [int suffixLen = 1])
      => parseInt(s.substring(0, s.length-suffixLen),
          onError: (s) => onError == null ? throw ex() : onError(s)); 
    
    if (s.endsWith('d')) return Duration(days: parsePrefix(s));
    if (s.endsWith('h')) return Duration(hours: parsePrefix(s));
    if (s.endsWith('m')) return Duration(minutes: parsePrefix(s));
    if (s.endsWith('s')) return Duration(seconds: parsePrefix(s));
    if (s.endsWith('ms')) return Duration(milliseconds: parsePrefix(s, 2));
    if (s.endsWith('us')) return Duration(microseconds: parsePrefix(s, 2));
    
    throw ex();
  }

  String format(Duration d) {
    if (_approx) d = _approximate(d);
    
    if (d.inMicroseconds == 0)
      return '0s';
    
    if (d.inMicroseconds % Duration.microsecondsPerMillisecond != 0)
      return '${d.inMicroseconds}us';

    if (d.inMilliseconds % Duration.millisecondsPerSecond != 0)
      return '${d.inMilliseconds}ms';

    if (d.inSeconds % Duration.secondsPerMinute != 0)
      return '${d.inSeconds}s';

    if (d.inMinutes % Duration.minutesPerHour != 0)
      return '${d.inMinutes}m';

    if (d.inHours % Duration.hoursPerDay != 0)
      return '${d.inHours}h';

    return '${d.inDays}d';
  }
  
  // Round up to the nearest unit.
  Duration _approximate(Duration d) {
    if (d.inMicroseconds == 0) return d;
    
    if (d > Duration(days: _threshold))
      return Duration(days: d.inDays);
    
    if (d > Duration(hours: _threshold))
      return Duration(hours: d.inHours);
    
    if (d > Duration(minutes: _threshold))
      return Duration(minutes: d.inMinutes);
    
    if (d > Duration(seconds: _threshold))
      return Duration(seconds: d.inSeconds);
    
    if (d > Duration(milliseconds: _threshold))
      return Duration(milliseconds: d.inMilliseconds);
    
    return Duration(microseconds: d.inMicroseconds);
  }

}
