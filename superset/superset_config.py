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
<<<<<<< HEAD
from datetime import datetime
import time
=======
import logging
from flask_appbuilder.security.manager import AUTH_OAUTH

logging.getLogger('requests').setLevel(logging.CRITICAL)
logging.getLogger('werkzeug').setLevel(logging.CRITICAL)
>>>>>>> eb61d40defbce7a41b047a5e74c6bcb990fa494e

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

OKTA_KEY = VAULT_SECRETS['OKTA_KEY']
OKTA_SECRET = VAULT_SECRETS['OKTA_SECRET']
OKTA_BASE_URL = VAULT_SECRETS['OKTA_BASE_URL']

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

    csv_row = "'{}', '{}', '{}', '{}', '{}', '{}'\n".format(
            str(curr_time),
            database,
            query.replace("\n", " "),
            schema,
            user,
            client
        )
    
    print(csv_row)
    date_str = str(datetime.now().date()).replace('-', '_')
    filepath = "/data/query_logs_{}.csv".format(date_str)

    # if data directory doesn't exist, create it
    if not os.path.isdir('/data/'):
        os.mkdir('/data/')
        print(csv_row)
    
    # if file does not exist, create it
    if not os.path.isfile(filepath):
        f = open(filepath, 'w+')
        f.write("time, database, sql, schema, user, client\n")
        f.close()

    with open(filepath, "a") as ql:
        print(csv_row)
        ql.write(csv_row)

# ====== Start Okta Login ===========
PUBLIC_ROLE_LIKE_GAMMA = True

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True  # allow self-registration (login creates a user)
AUTH_USER_REGISTRATION_ROLE = "Gamma"  # default is a Gamma user

# OKTA_BASE_URL must be
#    https://{yourOktaDomain}/oauth2/v1/ (okta authorization server)
# Cannot be
#    https://{yourOktaDomain}/oauth2/default/v1/ (custom authorization server)
# Otherwise you won't be able to obtain Groups info.
OAUTH_PROVIDERS = [{
    'name':'wishid.okta',
    'token_key': 'access_token', # Name of the token in the response of access_token_url
    'icon':'fa-circle-o',   # Icon for the provider
    'remote_app': {
        'consumer_key': OKTA_KEY,  # Client Id (Identify Superset application)
        'consumer_secret': OKTA_SECRET, # Secret for this Client Id (Identify Superset application)
        'request_token_params': {
            'scope': 'openid email profile groups'
        },
        'access_token_method': 'POST',    # HTTP Method to call access_token_url
        'base_url': OKTA_BASE_URL,
        'access_token_url': OKTA_BASE_URL + 'token',
        'authorize_url': OKTA_BASE_URL + 'authorize'
    }
}]

from superset.security import SupersetSecurityManager

logger = logging.getLogger('okta_login')

class CustomSsoSecurityManager(SupersetSecurityManager):

    def oauth_user_info(self, provider, response=None):
        if provider == 'wishid.okta':
            res = self.appbuilder.sm.oauth_remotes[provider].get('userinfo')
            if res.status != 200:
                logger.error('Failed to obtain user info: %s', res.data)
                return
            me = res.data
            logger.debug(" user_data: %s", me)
            prefix = 'Superset'
            groups = [
                x.replace(prefix, '').strip() for x in me['groups']
                if x.startswith(prefix)
            ]
            return {
                'username' : me['preferred_username'],
                'name' : me['name'],
                'email' : me['email'],
                'first_name': me['given_name'],
                'last_name': me['family_name'],
                # 'roles': groups, # TO-DO : Refactor AD groups and map to right role
                'roles' : ['Admin']
            }

    def auth_user_oauth(self, userinfo):
        user = super(CustomSsoSecurityManager, self).auth_user_oauth(userinfo)
        roles = [self.find_role(x) for x in userinfo['roles']]
        roles = [x for x in roles if x is not None]
        user.roles = roles
        logger.debug(' Update <User: %s> role to %s', user.username, roles)
        self.update_user(user)  # update user roles
        return user

CUSTOM_SECURITY_MANAGER = CustomSsoSecurityManager

# ====== End Okta Login ============
