# XQuery 2.0

## Setup 

### Dependencies 

- Linux Ubuntu 20.04 LTS (or similar)
- Python 3.8 or 3.9
- running PostgreSQL server
- running Redis server
- running Web3 enabled node (ETH, AVAX, etc.)

### Build Python 3.10 (not yet supported)

```shell
sudo apt install build-essential gdb lcov pkg-config \
      libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
      libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev \
      lzma lzma-dev tk-dev uuid-dev zlib1g-dev
```

```shell
mkdir /tmp/python && cd "$_"

# pull Python source
wget https://www.python.org/ftp/python/3.10.5/Python-3.10.5.tar.xz
tar xvf Python-3.*.tar.xz
cd Python-3.*/

# build 
./configure --enable-optimizations
make -j $(nproc)

# install 
# Note: `make install` can overwrite or masquerade the python3 binary. `make altinstall` is therefore recommended
sudo make altinstall
```

### Virtual Environment

```shell
sudo apt install python3-virtualenv
```

```shell
virtualenv -p python3 ./.venv
source .venv/bin/activate

pip install -r requirements.txt
```

For Python 3.10 use (not yet supported):
```shell
/usr/local/bin/python3.10 -m venv ./.venv
source .venv/bin/activate

pip install -U setuptools
pip install -r requirements.txt
```

> XQuery requires the `psycopg2` python package, which is compiled from source and thus has 
> additional system prerequisites (C compiler, system dev packages).
> See [here](https://www.psycopg.org/docs/install.html#install-from-source).

The required system packages can be install with:
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

Run the following commands to first create a migration and then apply it (create database tables).

```shell
alembic -n default -c alembic/alembic.ini revision --autogenerate -m 'creating schema'
alembic -n default -c alembic/alembic.ini upgrade head
```

### Verify Setup

Optionally, test the environment and configuration:

```shell
python -m test_setup
```

## Run Example

Run one of the preconfigured examples, Pangolin (PNG) Exchange on Avalanche or Pegasys (PSYS) Exchange on Syscoin:

```shell
python -m run_png
python -m run_psys
```

## Tests

> WARNING: Some tests currently affect the state of the cache and database. Only run on a development setup!

> Some tests only run on Avalanche (AVAX) currently

```shell
pytest --collect-only tests/

pytest -v tests/
pytest -v -rP tests/

pytest -v -k="cache" tests/
pytest -v -k="filter" tests/
pytest -v -k="indexer" tests/
pytest -v -k="middleware" tests/
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
