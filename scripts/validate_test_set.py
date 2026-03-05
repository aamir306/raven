import json
from collections import Counter

with open('tests/test_set_200.json') as f:
    data = json.load(f)

print(f'Total questions: {len(data)}')

ids = [q['id'] for q in data]
assert ids == list(range(1, 201)), 'IDs not sequential!'
print('IDs: sequential 1-200 OK')

simple = sum(1 for q in data if q['difficulty'] == 'SIMPLE')
comp = sum(1 for q in data if q['difficulty'] == 'COMPLEX')
print(f'SIMPLE: {simple} ({simple/len(data)*100:.0f}%)')
print(f'COMPLEX: {comp} ({comp/len(data)*100:.0f}%)')

cats = Counter(q['category'] for q in data)
print(f'\nCategories ({len(cats)}):')
for cat, count in sorted(cats.items(), key=lambda x: -x[1]):
    flag = ' WARN<10' if count < 10 else ' OK'
    print(f'  {cat}: {count}{flag}')

required = {'id', 'question', 'difficulty', 'expected_tables', 'category', 'notes'}
ok = True
for q in data:
    missing = required - set(q.keys())
    if missing:
        print(f'Q{q["id"]} missing: {missing}')
        ok = False
if ok:
    print('\nAll fields present OK')

bad_tables = []
for q in data:
    for t in q['expected_tables']:
        parts = t.split('.')
        if len(parts) != 3:
            bad_tables.append((q['id'], t))
if bad_tables:
    print(f'Bad table format: {bad_tables}')
else:
    print('All table names fully qualified OK')
