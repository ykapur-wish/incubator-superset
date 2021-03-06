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
import unittest

from superset.db_engine_specs.mysql import MySQLEngineSpec
from tests.db_engine_specs.base_tests import DbEngineSpecTestCase


class MySQLEngineSpecsTestCase(DbEngineSpecTestCase):
    @unittest.skipUnless(
        DbEngineSpecTestCase.is_module_installed("MySQLdb"), "mysqlclient not installed"
    )
    def test_get_datatype_mysql(self):
        """Tests related to datatype mapping for MySQL"""
        self.assertEqual("TINY", MySQLEngineSpec.get_datatype(1))
        self.assertEqual("VARCHAR", MySQLEngineSpec.get_datatype(15))
