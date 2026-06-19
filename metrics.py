import time


def average(values):
    if not values:
        raise ValueError("average requires at least one value")
    return sum(values) / len(values)


def median(values):
    if not values:
        raise ValueError("median requires at least one value")
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


def format_minute(window_start_ms):
    return time.strftime('%H:%M', time.localtime(window_start_ms / 1000))


def comfort_status(avg_temp, median_hum):
    if avg_temp is None or median_hum is None:
        return 'нет данных'
    labels = []
    if avg_temp >= 26:
        labels.append('жарко')
    elif avg_temp <= 18:
        labels.append('холодно')
    if median_hum >= 65:
        labels.append('влажно')
    elif median_hum <= 35:
        labels.append('сухо')
    if not labels:
        return 'комфортно'
    return ' + '.join(labels)


if __name__ == '__main__':
    assert average([20.0, 22.0, 24.0]) == 22.0
    assert median([30.0, 50.0, 40.0]) == 40.0
    assert median([10.0, 20.0, 30.0, 40.0]) == 25.0
    assert comfort_status(28.0, 40.0) == 'жарко'
    assert comfort_status(22.0, 70.0) == 'влажно'
    assert comfort_status(22.0, 50.0) == 'комфортно'
    assert comfort_status(28.0, 70.0) == 'жарко + влажно'
    print('metrics: все проверки пройдены')