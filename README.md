# XQuery 2.0

## Setup 

### Dependencies 

- Linux Ubuntu 20.04 LTS (or similar)
- Python 3.8 or 3.9
- running PostgreSQL server
- running Redis server
- running Web3 enabled node (ETH, AVAX, etc.)

### Virtual Environment

```shell
sudo apt install python3-virtualenv
```

```shell
virtualenv -p python3 ./.venv
source .venv/bin/activate

pip install -r requirements.txt
```

> XQuery requires the `psycopg2` python package, which is compiled from source and thus has 
> additional system prerequisites (C compiler, system dev packages).
> Details can be found [here](https://www.psycopg.org/docs/install.html#install-from-source).

The required system packages can be installed with:
```shell
sudo apt install build-essential python3-dev libpq-dev gcc
```

> Alternatively, install the precompiled `psycopg2-binary` python package instead.

### Configuration

All configurable settings are consolidated in `xquery/config.py`. Generally, no other files need to be modified.

The following options (with default value) are available and can be adjusted in the configuration file.
Alternatively, each option can also be set via its corresponding env variable. See details bellow:

```python
CONFIG = {
    # Database settings
    "DB_HOST": os.getenv("DB_HOST", "localhost"),
    "DB_PORT": os.getenv("DB_PORT", 5432),
    "DB_USERNAME": os.getenv("DB_USERNAME", "root"),
    "DB_PASSWORD": os.getenv("DB_PASSWORD", "password"),
    "DB_DATABASE": os.getenv("DB_DATABASE", "debug"),
    "DB_SCHEMA": os.getenv("DB_SCHEMA", "public"),

    # Redis cache settings
    "REDIS_HOST": os.getenv("REDIS_HOST", "localhost"),
    "REDIS_PORT": os.getenv("REDIS_PORT", 6379),
    "REDIS_PASSWORD": os.getenv("REDIS_PASSWORD", "password"),
    "REDIS_DATABASE": os.getenv("REDIS_DATABASE", 0),

    # Controller
    "XQ_NUM_WORKERS": os.getenv("XQ_NUM_WORKERS", 8),
    
    # web3 provider RPC url
    "API_URL": os.getenv("API_URL", "http://localhost:8545/"),
    # "API_URL": os.getenv("API_URL", "https://cloudflare-eth.com/v1/mainnet"),  # ETH
    # "API_URL": os.getenv("API_URL", "https://api.avax.network/ext/bc/C/rpc"),  # AVAX
    # "API_URL": os.getenv("API_URL", "https://rpc.syscoin.org/"),  # SYS
}
```

### Database

Run the following commands to first create a migration file in `alembic/versions/` and then apply it (create database tables).
More detailed documentation can be found [here](https://alembic.sqlalchemy.org/en/latest/tutorial.html#create-a-migration-script).

```shell
alembic -n default -c alembic/alembic.ini revision --autogenerate -m 'creating schema'
alembic -n default -c alembic/alembic.ini upgrade head
```

### Verify Setup

Optionally, test the environment and configuration (should complete without any errors):

```shell
python -m test_setup
```

## Run

### Basic Example

Run one of the preconfigured examples, Pangolin (PNG) Exchange on Avalanche (AVAX) or 
Pegasys (PSYS) Exchange on Syscoin (SYS):

```shell
python -m run_png
python -m run_psys
```

### Run Multiple Instances Simultaneously 

Concurrent instances of XQuery can be run in two different configurations. 
- Run a full stack for each instance (including a separate database with the default schema)
- Make use of the same database server, but create a separate schema for each instance

The latter is outlined in detail in the following section (can be adapted for any number instances).

Create a separate database schema for each XGraph by specifying the `DB_SCHEMA` env variable. 
This can be achieved by first creating two migrations and then applying them to the database:

```shell
DB_SCHEMA="xgraph_png" alembic -n default -c alembic/alembic.ini revision --autogenerate -m 'creating schema for Pangolin'
alembic -n default -c alembic/alembic.ini upgrade head
```

```shell
DB_SCHEMA="xgraph_psys" alembic -n default -c alembic/alembic.ini revision --autogenerate -m 'creating schema for Pegasys'
alembic -n default -c alembic/alembic.ini upgrade head
```

Once the necessary database structures have been created run each of the following lines in a separate terminal
in order to launch two simultaneous instances of XQuery:

```shell
DB_SCHEMA="xgraph_png" API_URL="http://localhost:9650/ext/bc/C/rpc" REDIS_DATABASE=0 python -m run_png
DB_SCHEMA="xgraph_psys" API_URL="http://localhost:8545/" REDIS_DATABASE=1 python -m run_psys
```

### Shutdown

The XQuery main process can handle the `SIGINT` (interrupt) and `SIGTERM` (terminate) POSIX signals. 
Both these signals can be used to initiate a graceful shutdown (might take several minutes). 

> Worker processes are fully managed by the main process and should never be terminated manually. 
> Doing so can lead to unrecoverable errors.

## Implement an XGraph

An XGraph consists of the following elements: An event `filter`, `indexer` and `processor` as well as an 
`orm` (object relational mapping) of the relevant tables.

- `filter`: Should inherit from the `EventFilter` base class and implement the necessary abstract methods.
- `indexer`: Should inherit from the `EventIndexer` base class and implement the necessary abstract methods.
- `processor`: Should inherit from either the `EventProcessor` or `EventProcessorStage`base class and implement
the necessary abstract methods. Depending on the complexity of the XGraph, the computation/processing might 
require multiple stages.
- `orm`: Any table should inherit from the `Base` (and optionally `BaseModel`) base class.

For more detailed documentation as well as assumptions/restrictions/responsibilities see the in code 
class descriptions. The relevant files can be found in `event/` and `db/orm/`.

The XGraph implementations for the Pangolin and Pegasys exchanges can serve as a reference.

## Tests

> WARNING: Some tests affect the state of the cache and database. Only run on a development setup!

> Some tests only run on Avalanche (AVAX) currently

```shell
pytest --collect-only tests/

pytest -v tests/
pytest -v -rP tests/

pytest -v -k="cache" tests/
pytest -v -k="dbm" tests/
pytest -v -k="decimal" tests/
pytest -v -k="filter" tests/
pytest -v -k="indexer" tests/
pytest -v -k="interval" tests/
pytest -v -k="middleware" tests/
pytest -v -k="processor" tests/
```

## Benchmarks

```shell
python -m bench.bench_cache_redis
python -m bench.bench_fetch_token
python -m bench.bench_fetch_token_batched
python -m bench.bench_get_block
python -m bench.bench_get_block_batched
python -m bench.bench_get_logs
python -m bench.bench_serialize
```
