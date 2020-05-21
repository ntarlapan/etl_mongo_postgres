import os


def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)


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
