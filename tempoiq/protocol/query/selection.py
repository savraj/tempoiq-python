class Compound(object):
    def __init__(self):
        self.selectors = []

    def add(self, selector):
        self.selectors.append(selector)


class AndClause(Compound):
    pass


class OrClause(Compound):
    pass


class ScalarSelector(object):
    def __init__(self, selection_type, key, value):
        self.selection_type = selection_type
        self.key = key
        self.value = value


class Selectable(object):
    def __init__(self, selection_type, key):
        self.selection_type = selection_type
        self.key = key


class ScalarSelectable(Selectable):
    def __init__(self, selection_type, key):
        self.selection_type = selection_type
        self.key = key

    def __eq__(self, other):
        return ScalarSelector(self.selection_type, self.key, other)


class ItemProxy(object):
    def __init__(self, selection_type, name, key):
        self.selection_type = selection_type
        self.name = name
        self.key = key

    def __eq__(self, other):
        return ScalarSelector(self.selection_type, self.name,
                              {self.key: other})


class DictSelectable(Selectable):
    def __init__(self, selection_type, key):
        self.selection_type = selection_type
        self.key = key

    def __getitem__(self, key):
        return ItemProxy(self.selection_type, self.key, key)


def and_(selectors):
    s = AndClause()
    for selector in selectors:
        s.add(selector)


def or_(selectors):
    s = OrClause()
    for selector in selectors:
        s.add(selector)
