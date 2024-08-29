import datetime
from airflow.decorators import task, dag


@dag(schedule=None, start_date=datetime.datetime(2024, 6, 1), tags=["test"])
def first_hello():

    @task
    def say_hello_task():
        print(
            "--------------------------->>>>hello<<<<<---------------------------------"
        )
        print(
            "--------------------------->>>>hello<<<<<---------------------------------"
        )
        print(
            "--------------------------->>>>hello<<<<<---------------------------------"
        )
        print(
            "--------------------------->>>>hello<<<<<---------------------------------"
        )

    say_hello_task()


first_dag = first_hello()

# if __name__ == '__main__':
#     first_dag.test()
