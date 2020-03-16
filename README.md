# Data Wrapper Manipulation
A Python Data Manipulation Wrapper (Pandas and Spark).
It's a wrapper to increase productivity, enable code reusability, with native best practices.

## Usage

Add this lib in your requirements.txt, than import It!

### Pandas

#### Load DataFrame from AWS Athena
```
import awswrangler
import pandas_dwm as pd_dwm

awswrangler_session = awswrangler.Session()
QUERY = 'SELECET * FROM foo;'
DATABASE = "db"

df = pd_dwm.read_athena(awswrangler_session, QUERY, DATABASE)
```

### Spark (coming soon!)

