# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
import os
import json
from celery.schedules import crontab

# Function to get the environmental Variables
def get_env_variable(var_name, default=None):
    """Get the environment variable or raise exception."""
    try:
        return os.environ[var_name]
    except KeyError:
        if default is not None:
            return default
        else:
            error_msg = 'The environment variable {} was missing, abort...'\
                        .format(var_name)
            raise EnvironmentError(error_msg)

# Reading Credentials from Vault
def read_vault():
    """Reads the vault json dump and extracts the variable"""
    vault_file = '/vault/secrets.json'
    secret_string = open(vault_file).read()
    secrets = json.loads(secret_string)
    return secrets

# Read vault secrets into constant
VAULT_SECRETS = read_vault()

MYSQL_USER = VAULT_SECRETS['MYSQL_USER']
MYSQL_PASSWORD = VAULT_SECRETS['MYSQL_PASSWORD']
MYSQL_HOST = VAULT_SECRETS['MYSQL_HOST']
MYSQL_PORT = VAULT_SECRETS['MYSQL_PORT']
MYSQL_DB = VAULT_SECRETS['MYSQL_DB']
REDIS_HOST = VAULT_SECRETS['REDIS_HOST']
REDIS_PORT = VAULT_SECRETS['REDIS_PORT']

# Superset Webserver
SUPERSET_WEBSERVER_TIMEOUT = 300

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = 'mysql://%s:%s@%s:%s/%s' % (MYSQL_USER,
                                                           MYSQL_PASSWORD,
                                                           MYSQL_HOST,
                                                           MYSQL_PORT,
                                                           MYSQL_DB)

# Celery Config
class CeleryConfig(object):
    BROKER_URL = 'redis://%s:%s/0' % (REDIS_HOST, REDIS_PORT)
    CELERY_IMPORTS = ('superset.sql_lab', 'superset.tasks')
    CELERY_RESULT_BACKEND = 'redis://%s:%s/1' % (REDIS_HOST, REDIS_PORT)
    CELERYD_LOG_LEVEL = 'DEBUG'
    CELERY_TASK_PROTOCOL = 1
    CELERYD_PREFETCH_MULTIPLIER = 10
    CELERY_ACKS_LATE = False
    CELERY_ANNOTATIONS = {
        "sql_lab.get_sql_results": {"rate_limit": "100/s"},
        "email_reports.send": {
            "rate_limit": "1/s",
            "time_limit": 120,
            "soft_time_limit": 150,
            "ignore_result": True,
        },
    }
    CELERYBEAT_SCHEDULE = {
        "email_reports.schedule_hourly": {
            "task": "email_reports.schedule_hourly",
            "schedule": crontab(minute=1, hour="*"),
        }
    }

CELERY_CONFIG = CeleryConfig

# Caching Config
CACHE_CONFIG = {
    'CACHE_TYPE': 'redis',
    'CACHE_DEFAULT_TIMEOUT': 60 * 60 * 24, # 1 day default (in secs)
    'CACHE_KEY_PREFIX': 'superset_results',
    'CACHE_REDIS_URL': 'redis://localhost:6379/0',
}

# On Redis
from werkzeug.contrib.cache import RedisCache
RESULTS_BACKEND = RedisCache(host='localhost', port=6379, key_prefix='superset_results')

# To allow Tahoe-Presto 
RESULTS_BACKEND_USE_MSGPACK = False

# To add suffix of impersonated user
from datetime import datetime
def SQL_QUERY_MUTATOR(sql, username, security_manager, database):
    dttm = datetime.now().isoformat()
    formatted_query = f"{sql} \n-- [Superset] {username} {dttm}"
    return formatted_query

#### CONFIGURING QUERY LOGGER
def QUERY_LOGGER(
 database,
 query,
 schema=None,
 user=None,
 client=None,
 security_manager=None,
):
    curr_time = int(time.time())

    csv_row = "{}, '{}', '{}', '{}', '{}', '{}'\n".format(
            curr_time,
            database,
            query.replace("\n", " "),
            schema,
            user,
            client
        )

    date_str = str(datetime.now().date()).replace('-', '_')
    filepath = "/data/query_logs_{}.csv".format(date_str)
    with open(filepath, "a") as ql:
        ql.write(csv_row)