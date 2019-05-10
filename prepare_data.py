from random import randint
from datetime import datetime, timedelta

def generate_two_col_data(fh, length):
    fh.write('x,y,z\n')
    for category in ('ham', 'spam'):
        step = datetime(1970, 1, 1, 1)
        for i in range(length):
            value = randint(0, 10**9)
            ts = hex(round(step.timestamp()))[2:]
            fh.write(f'{category},{ts},{value}\n')
            step = step + timedelta(hours=1)



with open('test-data/big-two-cols.csv', 'w') as fh:
    generate_two_col_data(fh, 10_000)

with open('test-data/small-two-cols.csv', 'w') as fh:
    generate_two_col_data(fh, 3)

# with open('test-data/small-one-col.csv', 'w') as fh:
#     fh.write('x,z\n')
#     step = datetime(1970, 1, 1)
#     for i in range(10_000):
#         value = randint(0, 10**9)
#         fh.write(f'{step},{value}\n')
#         step = step + timedelta(hours=1)
