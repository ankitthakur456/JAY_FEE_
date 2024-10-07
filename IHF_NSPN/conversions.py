import datetime
from datetime import timedelta
import logging

log = logging.getLogger("LOGS")

shift_a_start = datetime.time(7, 00, 0, 0)  # shift A is 7:00 to 15:00
shift_b_start = datetime.time(15, 0, 0, 0)  # Shift B is 15:00 to 7:00
shift_c_start = datetime.time(23, 0, 0, 0)  # Shift c is 23:00 to 7:00

shift_a_end = datetime.time(15, 0, 0, 0)
shift_b_end = datetime.time(23, 00, 0, 0)
shift_c_end = datetime.time(7, 0, 0, 0)


def get_shift():
    global shift_a_start, shift_b_start, shift_c_start, shift_a_end, shift_b_end, shift_c_end
    now = datetime.datetime.now().time()
    new_day = datetime.time(23, 59, 59, 999)
    new_one = datetime.time(0, 0, 0, 0)
    if shift_b_start <= now < shift_b_end:
        return 'B'
    elif shift_a_start <= now < shift_a_end:
        return 'A'
    elif shift_c_start <= now <= new_day or new_one <= now <= shift_c_end:
        return 'C'
    # else:
    #     # here returning C because we are not handling 0.001 microsecond in case of c shift that will return
    #     # None otherwise
    #     return 'B'


def break_check(current_shift):
    planned_break_dict = {
        #
        "A": {
            # "moving_1": [datetime.datetime.now().replace(hour=6, minute=30), 5],
            "tea_1": [datetime.datetime.now().replace(hour=9, minute=10), 10],
            "lunch": [datetime.datetime.now().replace(hour=11, minute=0), 30],
            "tea_2": [datetime.datetime.now().replace(hour=12, minute=20), 10],
            # "moving_2": [datetime.datetime.now().replace(hour=14, minute=40), 20],
        },
        "B": {
            # "moving_1": [datetime.datetime.now().replace(hour=15, minute=0), 5],
            "tea_1": [datetime.datetime.now().replace(hour=15, minute=41), 10],
            "lunch": [datetime.datetime.now().replace(hour=19, minute=0), 30],
            "tea_2": [datetime.datetime.now().replace(hour=20, minute=20), 10],
            # "moving_2": [datetime.datetime.now().replace(hour=23, minute=10), 20],
        },
        "C": {
            # "moving_1": [datetime.datetime.now().replace(hour=23, minute=30), 5],
            "tea_1": [datetime.datetime.now().replace(hour=23, minute=10), 10],
            "tea_2": [datetime.datetime.now().replace(hour=2, minute=10), 10],
            "tea_3": [datetime.datetime.now().replace(hour=4, minute=10), 10],
            # "moving_2": [datetime.datetime.now().replace(hour=6, minute=10), 20]
        },
    }
    # }
    current_time = datetime.datetime.now()
    break_time_dict = planned_break_dict[current_shift]

    for key, [planned_break, minutes] in break_time_dict.items():
        if (current_time - planned_break).total_seconds() < minutes * 60 and current_time > planned_break:
            return True
    return False
