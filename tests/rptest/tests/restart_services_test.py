# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import requests

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from rptest.services.admin import Admin
from rptest.services.redpanda import ResourceSettings, LoggingConfig, SchemaRegistryConfig

log_config = LoggingConfig('info',
                           logger_levels={
                               'admin_api_server': 'trace',
                               'security': 'trace',
                               'pandaproxy': 'trace',
                               'kafka/client': 'trace'
                           })


class RestartServicesTest(RedpandaTest):
    #
    # Smoke test the redpanda-services/restart Admin API endpoint
    #
    def __init__(self, context, **kwargs):
        super(RestartServicesTest, self).__init__(
            context,
            extra_rp_conf={"auto_create_topics_enabled": False},
            resource_settings=ResourceSettings(num_cpus=1),
            log_config=log_config,
            schema_registry_config=SchemaRegistryConfig(),
            **kwargs)

    @cluster(num_nodes=3)
    def test_restart_services(self):
        admin = Admin(self.redpanda)

        # Failure checks
        self.logger.debug("Check restart with no service name")
        try:
            admin.redpanda_services_restart()
        except requests.exceptions.HTTPError as ex:
            self.logger.debug(ex)
            assert ex.response.status_code == requests.codes.bad_request

        self.logger.debug("Check restart with invalid service name")
        try:
            admin.redpanda_services_restart(rp_service='foobar')
        except requests.exceptions.HTTPError as ex:
            self.logger.debug(ex)
            assert ex.response.status_code == requests.codes.not_found

        self.logger.debug("Check schema registry restart")
        result_raw = admin.redpanda_services_restart(
            rp_service='schema-registry')
        self.logger.debug(result_raw)
        assert result_raw.status_code == requests.codes.ok