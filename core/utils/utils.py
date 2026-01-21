# Copyright 2021 TradersOfTheUniverse S.A. All Rights Reserved.
#
# [Market Analyzer - This class looks for signals in the market]
#
# Authors:
#   antoniojose.luqueocana@telefonica.com
#   joseluis.roblesurquiza@telefonica.com
#   franciscojavier.gonzalezfernandez1@telefonica.com
#
# Version: 0.1
#

import pytz
import time
import datetime as dt


def datetime_utc_to_madrid(origin_datetime: dt.datetime) -> dt.datetime:
    """Transform an utc datetime to Europe/Madrid"""
    origin_tz = pytz.UTC
    dest_tz = pytz.timezone("Europe/Madrid")
    return origin_tz.localize(origin_datetime).astimezone(dest_tz).replace(tzinfo=None)


def datetime_madrid_to_utc(origin_datetime: dt.datetime) -> dt.datetime:
    """Transform an Europe/Madrid datetime to utc"""
    origin_tz = pytz.timezone("Europe/Madrid")
    dest_tz = pytz.UTC
    return origin_tz.localize(origin_datetime).astimezone(dest_tz).replace(tzinfo=None)


def round_seconds(my_datetime: dt.datetime) -> dt.datetime:
    if my_datetime.microsecond >= 500_000:
        my_datetime += dt.timedelta(seconds=1)
    return my_datetime.replace(microsecond=0)


def datetime_utc_to_unix_time(utc_datetime, milliseconds=False) -> float:
    unix_time = utc_datetime.replace(tzinfo=pytz.UTC).timestamp()
    if milliseconds:
        unix_time = int(unix_time * 1000)
    return unix_time


def unix_time_to_datetime_utc(unix_time) -> dt.datetime:
    """Detect if unix_time is seconds or milliseconds, and transforms it to datetime"""

    # If milliseconds, transforms to seconds
    if unix_time > time.time()*100:
        unix_time /= 1000

    return dt.datetime.utcfromtimestamp(unix_time)


def round_from_first_non_zero_number(origin_number, digits_to_round=2):
    left_zeros = 0
    for digit in str(abs(origin_number)):
        if digit == "0":
            left_zeros += 1
        elif digit != ".":
            break

    return round(origin_number, digits_to_round + left_zeros)


def reduce_decimals_from_total_digits(origin_number, max_total_digits):

    # No decimals
    if int(origin_number) == origin_number:
        return origin_number

    number_abs_str = str(abs(origin_number))

    # Reduce all decimals
    if number_abs_str.index(".") > max_total_digits:
        return int(origin_number)

    # Reduce some decimals

    extra_digits = 0
    if abs(origin_number) < 1:
        for digit in str(abs(origin_number)):
            if digit == "0":
                extra_digits += 1
            elif digit != ".":
                break

    return round(origin_number, max_total_digits - number_abs_str.index(".") + extra_digits)


def percentage_to_str(percentage: float, plus_symbol: bool = True):
    percentage_str = str(round(percentage * 100, 2)) + "%"
    if plus_symbol and percentage > 0:
        percentage_str = "+" + percentage_str
    return percentage_str


# Local Testing
if __name__ == "__main__":
    print(reduce_decimals_from_total_digits(14475111, 1))
    print(reduce_decimals_from_total_digits(1447.5111, 1))
    print(reduce_decimals_from_total_digits(144.75111, 1))
    print(reduce_decimals_from_total_digits(14.475111, 1))
    print(reduce_decimals_from_total_digits(1.4475111, 1))
    print(reduce_decimals_from_total_digits(0.15475111, 1))
    print(reduce_decimals_from_total_digits(0.000134475111, 1))
    print(reduce_decimals_from_total_digits(-14475111, 1))
    print(reduce_decimals_from_total_digits(-1447.5111, 1))
    print(reduce_decimals_from_total_digits(-144.75111, 1))
    print(reduce_decimals_from_total_digits(-14.475111, 1))
    print(reduce_decimals_from_total_digits(-1.4475111, 1))
    print(reduce_decimals_from_total_digits(-0.14475111, 1))
    print(reduce_decimals_from_total_digits(-0.000134475111, 1))
    print()

    print(reduce_decimals_from_total_digits(14475111, 2))
    print(reduce_decimals_from_total_digits(1447.5111, 2))
    print(reduce_decimals_from_total_digits(144.75111, 2))
    print(reduce_decimals_from_total_digits(14.475111, 2))
    print(reduce_decimals_from_total_digits(1.4475111, 2))
    print(reduce_decimals_from_total_digits(0.14475111, 2))
    print(reduce_decimals_from_total_digits(0.000134475111, 2))
    print(reduce_decimals_from_total_digits(-14475111, 2))
    print(reduce_decimals_from_total_digits(-1447.5111, 2))
    print(reduce_decimals_from_total_digits(-144.75111, 2))
    print(reduce_decimals_from_total_digits(-14.475111, 2))
    print(reduce_decimals_from_total_digits(-1.4475111, 2))
    print(reduce_decimals_from_total_digits(-0.14475111, 2))
    print(reduce_decimals_from_total_digits(-0.000134475111, 2))
    print()

    print(round_from_first_non_zero_number(14475, 1))
    print(round_from_first_non_zero_number(1.4475, 1))
    print(round_from_first_non_zero_number(0.14475, 1))
    print(round_from_first_non_zero_number(0.000134475, 1))
    print(round_from_first_non_zero_number(-14475, 1))
    print(round_from_first_non_zero_number(-1.4475, 1))
    print(round_from_first_non_zero_number(-0.14475, 1))
    print(round_from_first_non_zero_number(-0.000134475, 1))
    print()

    a = dt.datetime.utcnow()
    print(a)
    a = datetime_utc_to_unix_time(a)
    print(a)
    a = unix_time_to_datetime_utc(a)
    print(a)
    a = datetime_utc_to_madrid(a)
    print(a)
    a = datetime_madrid_to_utc(a)
    print(a)
    a = datetime_utc_to_unix_time(a)
    print(a)
    a = unix_time_to_datetime_utc(a)
    print(a)
    a = datetime_utc_to_madrid(a)
    print(a)

