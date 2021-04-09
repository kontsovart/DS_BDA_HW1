import random
import argparse
import time
from collections import Iterator


class RowsIterator(Iterator):

    def __init__(self, number_rows, step, start_time, number_devices, number_errors):
        self.__number_rows = number_rows
        self.__step = step
        self.__start_time = start_time
        self.__number_devices = number_devices
        self.__number_errors = number_errors
        self.__cur_row = start_time
        self.__cur_pos = 0
        self.__errors_pos = [random.randint(0, number_rows - 1) for _ in range(number_errors)]

    def __next__(self):
        if self.__cur_pos + 1 > self.__number_rows:
            raise StopIteration
        self.__cur_pos += 1
        if self.__cur_pos in self.__errors_pos:
            return get_errors_row()
        self.__cur_row += step
        return f"{random.randint(1, self.__number_devices)},{self.__cur_row},{random.randint(1, 100)}"


def current_milli_time():
    return round(time.time() * 1000)


def get_start_time(number_rows, step):
     # return random.randint(0, current_milli_time()-number_rows*step)
     return current_milli_time()


def get_number_rows():
    return random.randint(10000, 1000000)


def get_step():
    return random.randint(1, 1000*60*60*24)


def get_number_devices():
    return random.randint(2, 10)


def get_number_errors(number_rows):
    return random.randint(0, number_rows//10)


def get_errors_row():
    number_args = random.randint(1, 3)
    if number_args == 1:
        return f"{random.randint(0, 777)}"
    elif number_args == 2:
        return ",".join([str(random.randint(0, 777)), random.choice("pshepshe, rirva, pipidol")])
    return ",".join([str(random.randint(0, 777)),
                     str(random.choice("pshepshe, rirva, pipidol")),
                     str(random.randint(0, 100))])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start_time', type=str,
                        help='start time')
    parser.add_argument('-n', '--number_rows', type=str,
                        help='number rows')
    parser.add_argument('-t', '--step', type=str,
                        help='step')
    parser.add_argument('-d', '--number_devices', type=str,
                        help='number devices')
    parser.add_argument('-e', '--errors', type=str,
                        help='invalid rows count')

    args = parser.parse_args()
    if not args.number_rows:
        number_rows = get_number_rows()
    else:
        number_rows = args.number_rows
    if not args.step:
        step = get_step()
    else:
        step = args.step
    if not args.start_time:
        start_time = get_start_time(number_rows, step)
    else:
        start_time = args.start_time
    if not args.number_devices:
        number_devices = get_number_devices()
    else:
        number_devices = args.number_devices
    if not args.errors:
        number_errors = get_number_errors(number_rows)
    else:
        number_errors = args.errors

    genius_iterator = RowsIterator(number_rows, step, start_time, number_devices, number_errors)
    with open("output_data", 'w') as output_file:
        for row in genius_iterator:
            output_file.write(row + '\n')
    with open("metric_resolver", 'w') as metric_resolver:
        for item in range(1, number_devices):
            metric_resolver.write(f"{item},device_{item}" + '\n')
