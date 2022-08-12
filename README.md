# Запуск bootstrap
```shell
cp config.dist.yaml config.yaml
poetry install
export PYTHONPATH=$PWD
poetry run python app/bootstrap_app.py
```