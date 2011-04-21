import copy

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