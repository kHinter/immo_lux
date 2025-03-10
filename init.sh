#!/bin/bash

airflow_init()
{
    sudo apt update
    sudo apt install python3-pip
    sudo apt install python3-venv

    #The directory where init.sh is located
    BASH_DIR=$(dirname $(readlink -f $0))

    cd $BASH_DIR/..

    # Verify if the virtual environnment is already created
    if [[ ! -f ./venv/bin/activate ]]; then
        echo "Creation of a new virtual environment"
        python3 -m venv venv
    else
        echo "Virtual environment already exists"
    fi

    #Activate the venv
    source venv/bin/activate

    pip install airflowctl

    cd $BASH_DIR

    airflowctl init .

    echo "" >> .env
    echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow" >> .env
    echo "AIRFLOW__CORE__EXECUTOR=LocalExecutor" >> .env

    airflowctl build
    airflowctl start --background
}

airflow_init