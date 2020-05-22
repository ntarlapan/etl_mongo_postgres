import os

from dotenv import load_dotenv


def load_env_variables_from_file(env_file='.env'):
    """read the environment variables from .env file"""
    # Create .env file path.
    dotenv_path = os.path.join(os.path.dirname(__file__), env_file)
    # Load file from the path.
    load_dotenv(dotenv_path)


def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise KeyError(message)


def database_exists(engine, database_name):
    """
    check if the database with database_name exists in postgres
    """
    stmt = 'SELECT datname FROM pg_database WHERE datistemplate = false;'
    with engine.connect() as connection:
        results = connection.execute(stmt).fetchall()
    for record in results:
        if record[0] == database_name:
            return True

    return False
