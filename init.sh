#!/bin/bash

airflow_init()
{
    sudo apt update
    sudo apt install python3-pip
    sudo apt install sqlite3
    sudo apt install python3-venv

    cd ~

    #Verify if the virtual environnment is already created
    if [[ ! -f ./venv/bin/activate ]]; then
        echo "Creation of a new virtual environment"
        python3 -m venv venv
    fi

    source venv/bin/activate
    sudo apt-get install libpq-dev

    #Install airflow working with postgresql database
    pip install "apache-airflow[postgres]==2.5.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.0/constraints-3.7.txt"
    
    airflow db init
    sudo apt-get install postgresql postgresql-contrib

    #Execute PosgreSQL commands
    sudo -i -u postgres psql << EOF
    CREATE DATABASE airflow;
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    ALTER DATABASE airflow OWNER TO airflow;
EOF

    cd airflow

    #Switch from sqlite to postgresql
    sed -i 's#sqlite:////[^/]*\/airflow\/airflow.db#postgresql+psycopg2://airflow:airflow@localhost/airflow#g' airflow.cfg
    #Allow tasks to run in parallel
    sed -i 's#SequentialExecutor#LocalExecutor#g' airflow.cfg

    #Initalize again to effectively use PostgreSQL
    airflow db init
    airflow users create -u airflow -f airflow -l airflow -r Admin -e airflow@gmail.com -p airflow

    #Run airflow
    airflow webserver &
    airflow scheduler
}

selenium_init()
{
    wget -P . https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
    sudo dpkg -i google-chrome*.deb

    #Extract the chrome version
    CHROME_VERSION=$(google-chrome --version | grep -o '\([0-9]\+.\)\{3\}[0-9]\+')
    curl -O "https://storage.googleapis.com/chrome-for-testing-public/${CHROME_VERSION}/linux64/chromedriver-linux64.zip"
    unzip chromedriver-linux64.zip
    rm chromedriver-linux64.zip
    
    chmod +x chromedriver
    sudo mv chromedriver /usr/local/share/chromedriver
    sudo ln -s /usr/local/share/chromedriver /usr/local/bin/chromedriver
    sudo ln -s /usr/local/share/chromedriver /usr/bin/chromedriver

    sudo apt install python3-selenium
}

# airflow_init
# selenium_init

#Move folders in airflow directory
BASH_DIR=$(dirname $(readlink -f $0))
ln -s $BASH_DIR/dags/ ~/airflow/
ln -s $BASH_DIR/include/ ~/airflow/