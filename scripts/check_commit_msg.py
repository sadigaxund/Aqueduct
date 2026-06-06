#!/usr/bin/env python3
"""pre-commit hook: enforces conventional commit message format."""
import sys
import re

msg = open(sys.argv[1]).read().strip()
if re.match(r'^(feat|fix|chore|docs|refactor|test|release)(\(.+\))?(!)?: .+', msg):
    sys.exit(0)
if msg == 'root: initialize Aqueduct project':
    sys.exit(0)
print(f'❌ Commit must match: type(scope): message')
print(f'   Valid types: feat, fix, chore, docs, refactor, test, release')
print(f'   Got: {msg}')
sys.exit(1)
