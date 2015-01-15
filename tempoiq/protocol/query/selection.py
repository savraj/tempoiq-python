class Compound(object):
    def __init__(self):
        self.selectors = []
        self.selection_type = None

    def add(self, selector):
        self.selectors.append(selector)


class AndClause(Compound):
    pass


class OrClause(Compound):
    pass


class ScalarSelector(object):
    """A condition to filter on. The recommended way to construct a ScalarSelector
    is with the ``==`` operator on a selectable field, e.g::

        selector = (Device.attributes['model'] == "v1")
        selector2 = (Sensor.key == "humidity")
    """
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


class Selection(object):
    def __init__(self):
        self.selection = None

    def add(self, selector):
        if self.selection is None:
            self.selection = selector
        else:
            if issubclass(selector.__class__, Compound):
                self.selection.add(selector)
            else:
                clause = AndClause()
                clause.add(self.selection)
                clause.add(selector)
                self.selection = clause


def and_(selectors):
    """Returns a selector that's the AND of all provided selectors.

    :param selectors:
    :type selectors: List of :class:`ScalarSelector`"""
    s = AndClause()
    object_type = None
    for selector in selectors:
        if object_type is None:
            object_type = selector.selection_type
        s.add(selector)
    s.selection_type = object_type
    return s


def or_(selectors):
    """Returns a selector that's the OR of all provided selectors.

    :param selectors:
    :type selectors: List of :class:`ScalarSelector`"""
    s = OrClause()
    object_type = None
    for selector in selectors:
        if object_type is None:
            object_type = selector.selection_type
        s.add(selector)
    s.selection_type = object_type
    return s
