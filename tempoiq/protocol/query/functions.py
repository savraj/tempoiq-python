class Function(object):
    def __init__(self, name, args):
        self.name = name
        self.args = args


class Aggregation(Function):
    def __init__(self, function):
        super(Aggregation, self).__init__('aggregation', [function])


class ConvertTZ(Function):
    def __init__(self, tz):
        super(ConvertTZ, self).__init__('convert_tz', [tz])


class Rollup(Function):
    def __init__(self, function, period, start=None):
        super(Rollup, self).__init__('rollup', [function, period, start])


class MultiRollup(Function):
    def __init__(self, functions, period, start=None):
        super(MultiRollup, self).__init__('multi_rollup', [functions, period, start])


class Find(Function):
    def __init__(self, function, period, start=None):
        super(Find, self).__init__('find', [function, period, start])


class Interpolation(Function):
    def __init__(self, function, period, start=None, end=None):
        super(Interpolation, self).__init__('interpolate', [function, period, start, end])


class APIOperation(object):
    def __init__(self, name, args):
        self.name = name
        self.args = args
