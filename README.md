## export the environment variables:

``
export POSTGRES_URL="127.0.0.1:5432"
export POSTGRES_USER="postgres"
export POSTGRES_PW="uGAn7agA"
export POSTGRES_DB="go_parrot"
``

## How to launch the cron job:
cd into the project's directory

create virtual environment

`python3 -m venv venv`

activate virtual enviroment:

`source venv/bin/activate`

### to show current crontab jobs:

flask crontab show

### To schedule the current crontab_job 
: 
flask crontab add

