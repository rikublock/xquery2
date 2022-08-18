# XQuery 2.0

## Setup 

### Dependencies 

- requires Python 3.8 or 3.9
- running PostgreSQL server
- running Redis server

### Build Python 3.10 (not yet supported)

```shell
sudo apt-get install build-essential gdb lcov pkg-config \
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

### Configuration

All configurable settings are consolidated in `xquery/config.py`. Generally, no other files need to be modified!

### Run Example

```shell
python -m run
```

## Database

Run the following commands to create the database tables.

```shell
alembic -n default -c alembic/alembic.ini revision --autogenerate -m 'creating schema'
alembic -n default -c alembic/alembic.ini upgrade head
```

## Tests

> WARNING: Some tests currently affect the state of the cache and database. Only run on a development setup!

```shell
pytest --collect-only tests/
pytest --collect-only -k="cache" tests/

pytest -v tests/
pytest -v -k="cache" tests/
```

## Benchmarks

```shell
python -m bench.bench_fetch_token
python -m bench.bench_fetch_token_batched
python -m bench.bench_get_block
python -m bench.bench_get_block_batched
python -m bench.bench_get_logs
python -m bench.bench_serialize
```
