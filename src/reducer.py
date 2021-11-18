import sys

word_count = dict()
for line in sys.stdin:
    line = line.strip()
    word, count = line.split(',', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    if word in word_count:
        word_count[word] += count
    else:
        word_count[word] = count

for word in word_count:
    print(word, word_count[word])
