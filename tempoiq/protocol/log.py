class RuleLog(object):
    def __init__(self, log_id, description, timestamp):
        self.log_id = log_id
        self.description = description
        self.timestamp = timestamp


class RuleUsageMetric(object):
    def __init__(self, timestamp, metric_name, metric_value):
        self.timestamp = timestamp
        self.metric_name = metric_name
        self.metric_value = metric_value


class RuleUsage(object):
    def __init__(self, timestamp):
        self.timestamp = timestamp

    def add_metric(self, metric_name, metric_value):
        setattr(self, metric_name, metric_value)
