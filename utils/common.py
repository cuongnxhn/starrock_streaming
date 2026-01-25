def calculate_max_offsets(expected_msg_per_day):
    """Tính max offsets per trigger dựa trên volume"""
    if expected_msg_per_day < 100000:
        return 10000
    elif expected_msg_per_day < 1000000:
        return 50000
    else:
        return 100000


def calculate_trigger_interval(expected_msg_per_day):
    """Tính trigger interval dựa trên volume"""
    if expected_msg_per_day < 100000:
        return "5 minutes"
    elif expected_msg_per_day < 1000000:
        return "2 minutes"
    else:
        return "1 minute"
