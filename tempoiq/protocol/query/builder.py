class QueryBuilder(object):
    def __init__(self, object_type):
        self.object_type = object_type
        self.selection = None
        self.pipeline = []
