from selection import Selection, ScalarSelector, OrClause, AndClause


class QueryBuilder(object):
    def __init__(self, client, object_type):
        self.client = client
        self.object_type = object_type
        self.selection = {
            'devices': Selection(),
            'sensors': Selection()
        }
        self.pipeline = []

    def filter(self, selector):
        if not isinstance(selector, (ScalarSelector, OrClause, AndClause)):
            raise TypeError('Invalid object for filter: "%s"' % selector)
        self.selection[selector.selection_type].add(selector)
        return self
