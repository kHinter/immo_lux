#!/bin/bash

airflow_init()
{
    sudo apt update
    sudo apt install python3-pip
    sudo apt install sqlite3
    sudo apt install python3-venv

    cd $BASH_DIR/..

    #Verify if the virtual environnment is already created
    if [[ ! -f ./venv/bin/activate ]]; then
        echo "Creation of a new virtual environment"
        python3 -m venv venv
    else
        echo "Virtual environment already exists"
    fi

    source venv/bin/activate
    sudo apt-get install libpq-dev

    #Set AIRFLOW_HOME variable for all users
    echo 'AIRFLOW_HOME="/home/airflow"' | sudo tee -a /etc/environment
    echo "AIRFLOW_HOME environment variable set to : {$AIRFLOW_HOME}"

    #Install airflow working with postgresql database
    pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
    
    #Required python libraries
    pip install selenium langchain_ollama ollama
    pip install pandas
    pip install bs4
    pip install opencv-python
    pip install matplotlib
    pip install Unidecode
    pip install openpyxl
    pip install torch torchvision timm
    
    airflow db init
    sudo apt-get install postgresql postgresql-contrib

    #Execute PosgreSQL commands
    sudo -i -u postgres psql << EOF
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    ALTER DATABASE airflow OWNER TO airflow;
EOF

    cd $AIRFLOW_HOME

    #Switch from sqlite to postgresql
    sed -i 's#sqlite:////[^/]*\/airflow\/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg
    #Allow tasks to run in parallel
    sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg

    #To avoid getting dagbag timeout error
    sed -i 's#dagbag_import_timeout = 30.0#dagbag_import_timeout = 60.0#g' airflow.cfg

    #Initalize again to effectively use PostgreSQL
    airflow db init
    airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com -p airflow

    #Run airflow
    # airflow webserver &
    # airflow scheduler &
}

BASH_DIR=$(dirname $(readlink -f $0))

airflow_init

#Move folders in airflow directory

ln -s $BASH_DIR/dags/ $AIRFLOW_HOME
ln -s $BASH_DIR/include/ $AIRFLOW_HOME