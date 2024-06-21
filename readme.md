[//]: # (export AIRFLOW_HOME=`readlink -f .`)
make install
export AIRFLOW_HOME=`readlink -f .`
airflow db init
airflow users  create --role Admin --username wudmc --email admin --firstname admin --lastname admin --password wudmc
https://www.youtube.com/watch?v=lYhag-yNtIQ