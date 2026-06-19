from pyflink.common.watermark_strategy import TimestampAssigner

def parse_iot(val):
    if not val or not val.strip():
        return None
    res = val.split(",")
    if len(res) == 4:
        try:
            device_id = int(res[0])
            event_ts = int(res[1])
            temperature = float(res[2])
            humidity = float(res[3])
            return device_id, event_ts, temperature, humidity
        except ValueError:
            return None
    return None


class IotTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]