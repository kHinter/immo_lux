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

    #Setup of AIRFLOW_HOME environment variable
    if ! grep -q "AIRFLOW_HOME" ~/.bashrc; then
        echo "export AIRFLOW_HOME=/home/airflow" >> ~/.bashrc
    fi

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
    pip install torch torchvision
    
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

get_linux_distribution()
{
    if grep -q "Ubuntu" /etc/os-release; then
        echo "ubuntu"
    elif grep -q "Debian" /etc/os-release; then
        echo "debian"
    elif grep -q "CentOS" /etc/os-release; then
        echo "centOS"
    elif grep -q "Fedora" /etc/os-release; then
        echo "fedora"
    elif grep -q "Red Hat" /etc/os-release; then
        echo "red hat"
    else
        echo "Other"
    fi
}

#Install docker for selenium hub
docker_init()
{
    #Checking if docker is already installed
    if command -v docker &> /dev/null; then
        echo "Docker is already installed"
    else
        LINUX_DISTRIBUTION=$(get_linux_distribution)
        case $LINUX_DISTRIBUTION in
            "ubuntu" | "debian")
                # Add Docker's official GPG key:
                sudo apt-get update
                sudo apt-get install ca-certificates curl
                sudo install -m 0755 -d /etc/apt/keyrings
                sudo curl -fsSL https://download.docker.com/linux/$LINUX_DISTRIBUTION/gpg -o /etc/apt/keyrings/docker.asc
                sudo chmod a+r /etc/apt/keyrings/docker.asc

                # Add the repository to Apt sources:
                echo \
                    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/$LINUX_DISTRIBUTION \
                    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
                    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
                sudo apt-get update

                 sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

                echo "Docker has been successfully installed"
                ;;
            *)
                echo "Distribution not supported"
                ;;
        esac
    fi
}

BASH_DIR=$(dirname $(readlink -f $0))

airflow_init
docker_init

#Move folders in airflow directory

ln -s $BASH_DIR/dags/ $AIRFLOW_HOME
ln -s $BASH_DIR/include/ $AIRFLOW_HOME