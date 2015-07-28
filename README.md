# nginx_log_2_sqlite
Parses a given nginx file (standard format) to a sqlite db. You probably have no need of this…


---

If you really want to know:

```bash
python parse.py access.log

```

Optionally add a name for the database

```bash
python parse.py access.log logs.sqlite3

```
