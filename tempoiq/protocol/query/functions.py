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
    def __init__(self, function, period):
        super(Rollup, self).__init__('rollup', [function, period])


class MultiRollup(Function):
    def __init__(self, functions, period):
        super(MultiRollup, self).__init__('multi_rollup', [functions, period])


class Find(Function):
    def __init__(self, function, period):
        super(Find, self).__init__('find', [function, period])


class Interpolation(Function):
    def __init__(self, function, period):
        super(Interpolation, self).__init__('interpolate', [function, period])


class APIOperation(object):
    def __init__(self, name, args):
        self.name = name
        self.args = args
