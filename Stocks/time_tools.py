import datetime
import pytz

def generate_timestamps(period: str, interval: str, timezone: str) -> list:
    '''
    Generate is of timestamps given a period and interal, or a start and end
    '''

    # Note: inconsistent use of dictionary index and .get() method
    # Need to accomodate for start and end as opposed to period and interval

    # Define time with respect to one day
    # !rename to minute_denom
    second_denom = 60
    day_denom = 24 * 60 * 60

    # Define market open and close time
    open_days = [0, 1, 2, 3, 4]
    open_time = datetime.time(9, 30)
    close_time = datetime.time(16, 30)

    # Define interval lengths
    interval_dict = {'1m': second_denom,
                        '2m': 2 * second_denom,
                        '5m': 5 * second_denom,
                        '15m': 15 * second_denom,
                        '30m': 30 * second_denom,
                        '60m': 60 * second_denom,
                        '90m': 90 * second_denom,
                        '1h': 60 * second_denom,
                        '1d': day_denom,
                        '5d': 5 * day_denom,
                        '1wk': 7 * day_denom,
                        '1mo': 30 * day_denom,
                        '3mo': 3 * 30 * day_denom
    }

    # Define period lengths
    period_dict = {'1h': 60 * second_denom,
                    '1d': 1 * day_denom,
                    '5d': 5 * day_denom,
                    '1mo': 1 * 30 * day_denom,
                    '3mo': 3 * 30 * day_denom,
                    '6mo': 6 * 30 * day_denom,
                    '1y': 365 * day_denom,
                    '2y': 2 * 365 * day_denom,
                    '5y': 5 * 365 * day_denom,
                    '10y': 10 * 365 * day_denom
    }

    # Set end time to now and as UTC
    end = datetime.datetime.now().astimezone(pytz.UTC)

    # Set end period

    # If the end time is greater than the close
    # time, set the end time to the close time
    if end.time() > close_time:
        end = end.replace(
                    hour=close_time.hour, 
                    minute=close_time.minute,
                    second=close_time.second,
                    microsecond=close_time.microsecond
                    )
    # If the end time is less than the open
    # time, set the end time to the close time
    # and subtract a day
    if end.time() < open_time:
        end = end.replace(day=end.day-1,
                            hour=close_time.hour,
                            minute=close_time.minute,
                            second=close_time.second,
                            microsecond=close_time.microsecond)

    # Get duration in seconds
    interval_seconds = interval_dict[interval]
    timestamps_list = []

    # Get largest time within interval from close time
    # to round the timestamps
    largest_time = datetime.datetime(
                            year=end.year,
                            month=end.month,
                            day=end.day,
                            hour=close_time.hour,
                            minute=close_time.minute,
                            second=close_time.second,
                            microsecond=close_time.microsecond)

    while largest_time.time() > end.time():
        largest_time = largest_time - datetime.timedelta(seconds=interval_seconds)
    
    # If different, save the end time in timestamps list
    # and set the new end to the rounded value
    if largest_time.time() != end.time():
        timestamps_list.append(end)
        end = end.replace(hour=largest_time.hour,
                    minute=largest_time.minute,
                    second=largest_time.second,
                    microsecond=largest_time.microsecond
        )

    # If period is more than or equal to one day,
    # set start to the open time and subtract
    # the number of days remaining
    if period_dict.get(period) >= day_denom:
        start = end.replace(hour=open_time.hour,
                        minute=open_time.minute,
                        second=open_time.second,
                        microsecond=open_time.microsecond)
        sub_days = (period_dict.get(period) / day_denom)  - 1
        start = start - datetime.timedelta(days=sub_days)

    # If period is less than one day, subtract
    # the period from the end time
    if period_dict.get(period) < day_denom:
        start = end - datetime.timedelta(seconds=period_dict[period])

    # Add all timestamps in period to list
    while start < end:
        if start.time() >= open_time and start.time() < close_time:
            timestamps_list.append(start)
        start += datetime.timedelta(seconds=interval_seconds)

    # Add end timestamp if not included
    if end not in timestamps_list:
        timestamps_list.append(end)

    # Get list of days
    days = set([timestamp.date() for timestamp in timestamps_list])
    days = sorted(list(days))[::-1]

    # Define closed days
    closed_days = [timestamp for timestamp in days if timestamp.weekday() not in open_days] 

    offset = 0
    new_day_dict = {day: None for day in days}

    # Replace days with weekend-adjusted days
    for day in days:
        new_day = day - datetime.timedelta(days=offset)
        if new_day.weekday() not in open_days:
            offset += new_day.weekday() - max(open_days)
        new_day = day - datetime.timedelta(days=offset)

        new_day_dict[day] = new_day
    
    # Adjust all days to not include weekends
    normalised_list = []
    for timestamp in timestamps_list:
        new_day = new_day_dict[timestamp.date()]
        new_timestamp = timestamp.replace(
            year=new_day.year,
            month=new_day.month,
            day=new_day.day
        )
        normalised_list.append(new_timestamp)

    return normalised_list
