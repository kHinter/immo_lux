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

    sed -i 's#/home#$BASH_DIR#g' settings.yaml

    airflowctl init .
    airflowctl build
    airflowctl start --background
}

airflow_init