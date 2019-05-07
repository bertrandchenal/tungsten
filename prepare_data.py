from datetime import datetime, timedelta

with open('test-data/basic-two-cols.csv', 'w') as fh:
    fh.write('x,y,z\n')
    for category in ('ham', 'spam'):
        step = datetime(1970, 1, 1)
        for i in range(10_000):
            fh.write(f'{category},{step},{i}\n')
            step = step + timedelta(hours=1)

with open('test-data/basic-one-col.csv', 'w') as fh:
    fh.write('x,z\n')
    step = datetime(1970, 1, 1)
    for i in range(10_000):
        fh.write(f'{step},{i}\n')
        step = step + timedelta(hours=1)
