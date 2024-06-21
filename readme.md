[//]: # (export AIRFLOW_HOME=`readlink -f .`)
make install
airflow db init
airflow users  create --role Admin --username wudmc --email admin --firstname admin --lastname admin --password wudmc
