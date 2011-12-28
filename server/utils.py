import copy
import datetime
import pytz
from types import *

class FatalError(Exception):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return repr(self.msg)

class TimeSlot(object):
    """Represent, manipulate time-slot"""
    def __init__(self, starttime, slotsize):
        super(TimeSlot, self).__init__()
        self.start = starttime
        self.slotsize = slotsize
        self.current = copy.deepcopy(self.start)

    def set_to_start(self, new_start=None):
        if new_start:
            self.start = new_start
        self.current = copy.deepcopy(self.start)
    
    def set_slotsize(self, slotsize):
        self.slotsize = slotsize

    def next(self, number_of_slots):
        end = [self.current[0], self.current[1] + (self.slotsize * number_of_slots)]
        end = [end[0] + end[1] // 60, end[1] % 60]
        start = copy.deepcopy(self.current)
        self.current = copy.deepcopy(end)
        return start, end

class DatetimeConverter(object):
    """docstring for DatetimeConverter"""
    def __init__(self, default_timezone, date_format = None, time_format = None):
        super(DatetimeConverter, self).__init__()

        # Timezone presets
        self.tz_utc = pytz.utc
        self.tz_eastern = pytz.timezone('US/Eastern')
        self.tz_central = pytz.timezone('US/Central')
        self.tz_seoul = pytz.timezone('Asia/Seoul')
        self.tz_list = [tz.lower() for tz in pytz.all_timezones]

        # Set default_timezone.
        # TODO: Need more adjustment to support more various timezone keywords.
        dtz = default_timezone.lower()
        self.dtz = self.parse_tz(dtz)
        
        # Set default formats
        self.date_format = date_format
        self.time_format = time_format
    
    def set_tz(self, timezone, source_dt_tuple):
        dt = self.parse_dt_tuple(source_dt_tuple)
        timezone = self.parse_tz(timezone)
        return timezone.localize(dt)

    def convert_tz(self, target_timezone, source_dt_tuple, source_timezone = None):
        source_dt = self.parse_dt_tuple(source_dt_tuple)
        if not source_timezone:
            source_timezone = self.dtz
        source_timezone = self.parse_tz(source_timezone)
        target_timezone = self.parse_tz(target_timezone)
        loc_dt = source_timezone.localize(source_dt)
        target_dt = loc_dt.astimezone(target_timezone)
        return target_dt

    def datetime_to_tuple(self, dt):
        return (dt.year, dt.month, dt.day, dt.hour, dt.minute)

    def convert_fmt(self, source_dt_tuple, target):
        # TODO: Implement a format converter
        pass

    def parse_tz(self, timezone):
        # Pass through if timezone is tzinfo object
        # else, find a valid timezone matched to input string 
        if isinstance(timezone, datetime.tzinfo):
            return timezone
        elif isinstance(timezone, str):
            ltz = timezone.lower()
            if (ltz == 'utc') or (ltz == 'gmt'):
                return pytz.utc
            elif (ltz == 'edt') or (ltz == 'est'):
                return self.tz_eastern
            elif ltz == 'cst':
                return self.tz_central
            elif ltz == 'kst':
                return self.tz_seoul
            elif ltz in self.tz_list:
                return pytz.timezone(ltz)
            raise pytz.exceptions.UnknownTimeZoneError(ltz)
        raise FatalError('Cannot parse timezone')

    def parse_dt_tuple(self, dt_tuple):
        if isinstance(dt_tuple, datetime.datetime):
            return dt_tuple
        assert isinstance(dt_tuple, tuple), "%s must be instance of tuple" % (dt_tuple,)
        length = len(dt_tuple)
        if length == 1:
            dt = dt_tuple[0]
            assert isinstance(dt, datetime.datetime), "%s must be instance of datetime" % (dt,)
            return dt
        elif length == 2:
            d, t = dt_tuple
            if isinstance(d, str):
                date = datetime.datetime.strptime(d, self.date_format).date()
            else:
                #assert (isinstance(d, list) or isinstance(d, tuple)) and len(d) == 3, "%s must be instance of list or tuple with length 3" % (d,)
                date = datetime.date(d[0], d[1], d[2])
            if isinstance(t, str):
                time = datetime.datetime.strptime(t, self.time_format).time()
            else: 
                #assert (isinstance(t, list) or isinstance(t, tuple)) and len(t) == 2, "%s must be instance of list or tuple with length 2" % (d,)
                time = datetime.time(t[0], t[1])
            return datetime.datetime.combine(date, time)
        elif length == 3:
            a, b, c = dt_tuple
            if isinstance(a, str):
                #assert isinstance(b, str) and (isinstance(c, list) or isinstance(c, tuple))
                date = datetime.datetime.strptime(d, b).date()
                time = datetime.time(c[0], c[1])
            else:
                #assert (isinstance(a, list) or isinstance(a, tuple)) and isinstance(b, str) and isinstance(c, str)
                date = datetime.date(a[0], a[1], a[2])
                time = datetime.datetime.strptime(b, c).time()
            return datetime.datetime.combine(date, time)
        elif length == 4:
            d, df, t, tf = dt_tuple
            # TODO: add asserts for type checking.
            if df == 'default':
                df = self.date_format
            if tf == 'default':
                tf = self.time_format
            date = datetime.datetime.strptime(d, df).date()
            time = datetime.datetime.strptime(t, tf).time()
            return datetime.datetime.combine(date, time)
        raise FatalError('Cannot parse datetime tuple - %s.' % (dt_tuple,))
    
    


