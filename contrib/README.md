# Optional Tools

> **Warning:** The scripts in this folder are not actively tested nor maintained and come without 
> **any** warranty or support (may very well contain bugs).
> 
> These scripts were merely added as convenience for developers and should under no circumstances 
> (in their current form) be used in production!


## Docker Containers

Credentials should be stored in the `.env` file (copy `.env.template`) and be placed in the `contrib/` folder. 
Temporary docker containers can then be launched from the `contrib/` directory by running:

```shell
./run.hasura.sh
./run.pg.sh
./run.redis.sh
```

Optionally, a very basic configuration can be applied to Hasura by using the following script
(needs to be run from the project root directory in a venv):

```shell
python -m contrib.init_hasura
```
