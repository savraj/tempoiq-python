import exceptions
import warnings

class TempoIQDeprecationWarning(exceptions.DeprecationWarning):
    pass

warnings.filterwarnings('default', category=TempoIQDeprecationWarning)
