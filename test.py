from rust_fst import Map


content = open('output.tun', 'rb').read()

idx_len = int.from_bytes(content[-2:], 'big')
print(idx_len)
idx_end = len(content) - 2
idx_start = idx_end - idx_len

with open('tmp.fst', 'wb') as tmp_fh:
    tmp_fh.write(content[idx_start:idx_end])

m = Map(path='tmp.fst')
items = list(m.items())
items.sort(key=lambda x: -x[1])
print(items)
offsets = [o for _, o in items]

for end, start in zip(offsets, offsets[1:]):
    if start == end:
        continue
    with open('tmp.fst', 'wb') as tmp_fh:
        tmp_fh.write(content[start:end])
    print(start, end)
    m = Map(path='tmp.fst')
    print(dict(m))

