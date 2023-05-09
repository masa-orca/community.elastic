#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2021, Rhys Campbell (@rhysmeister) <rhys.james.campbell@googlemail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: elastic_cluster_health

short_description: Validate cluster health.

description:
  - Validate cluster health.
  - Optionally wait for an expected status.

author:
  - Rhys Campbell (@rhysmeister)
  - ONODERA Masaru (@masa-orca)
version_added: "0.0.1"

extends_documentation_fragment:
  - community.elastic.login_options

options:
  level:
    description:
      - Controls the details level of the health information returned
    type: str
    choices:
      - cluster
      - indicies
      - shards
    default: cluster
  local:
    description:
      - If true, the request retrieves information from the local node only.
      - Defaults to false, which means information is retrieved from the master node.
    type: bool
    default: false
  master_timeout:
    description:
      - Period to wait for a connection to the master node.
      - This parameter is referred by cluster health API.
    type: str
    default: 30s
  wait_for_active_shards:
    description:
      - A number controlling to how many active shards wait for.
      - If you set C(all), this module waits for all shards in the cluster to be active.
    type: str
    default: "0"
  wait_for_events:
    description:
      - Waiting for all of currently queued events of the priority.
    type: str
    choices:
      - immediate
      - urgent
      - high
      - normal
      - low
      - languid
  wait_for_no_initializing_shards:
    description:
      - If you set C(true), this module waits for the cluster to have no shard initializations.
    type: bool
    default: false
  wait_for_no_relocating_shards:
    description:
      - If you set C(true), this module waits  for the cluster to have no shard relocations.
    type: bool
    default: false
  wait_for_nodes:
    description:
      - Waiting for a number of node is available.
      - Please set value like C(<=N) or C(lt(N)).
    type: str
  wait_for_status:
    description:
      - Expected status of the cluster changes to the one provided or better, i.e. green > yellow > red.
    type: str
    choices:
      - green
      - yellow
      - red
"""

EXAMPLES = r"""
- name: Validate cluster health
  community.elastic.elastic_cluster_health:

- name: Ensure cluster health status is green with 90 seconds timeout
  community.elastic.elastic_cluster_health:
    wait_for_status: "green"
    timeout: 90

- name: Ensure at least 10 nodes are up with 2m timeout
  community.elastic.elastic_cluster_health:
    wait_for_nodes: ">=10"
    timeout: 2m
"""

RETURN = r"""
"""


from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils._text import to_native


from ansible_collections.community.elastic.plugins.module_utils.elastic_common import (
    missing_required_lib,
    elastic_found,
    E_IMP_ERR,
    elastic_common_argument_spec,
    ElasticHelpers,
)
import time


def elastic_status(desired_status, cluster_status):
    """
    Return true if the desired status is equal to or less
    than the cluster status.
    """
    status_dict = {"red": 0, "yellow": 1, "green": 2}
    if status_dict[desired_status] <= status_dict[cluster_status]:
        return True
    else:
        return False


def cast_to_be(to_be):
    """
    Cast the value to int if possible. Otherwise return the str value
    """
    try:
        to_be = int(to_be)
    except ValueError:
        pass
    return to_be


# ================
# Module execution
#


def main():
    wait_for_events_values = ["immediate", "urgent", "high", "normal", "low", "languid"]

    argument_spec = elastic_common_argument_spec()
    argument_spec.update(
        level=dict(
            type="str", choices=["cluster", "indicies", "shards"], default="cluster"
        ),
        local=dict(type="bool", default=False),
        master_timeout=dict(type="str", default="30s"),
        wait_for_active_shards=dict(type="str", default="0"),
        wait_for_events=dict(type="str", choices=wait_for_events_values),
        wait_for_no_initializing_shards=dict(type="bool", default=False),
        wait_for_no_relocating_shards=dict(type="bool", default=False),
        wait_for_nodes=dict(type="str"),
        wait_for_status=dict(
            type="str", choices=["green", "yellow", "red"]
        ),
    )

    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=False,
        required_together=[
            ["login_user", "login_password"],
        ],
    )

    if not elastic_found:
        module.fail_json(msg=missing_required_lib("elasticsearch"), exception=E_IMP_ERR)

    try:
        elastic = ElasticHelpers(module)
        client = elastic.connect()

        params = {}
        params["level"] = module.params["level"]
        if module.params["local"]:
            params["local"] = "true"
        else:
            params["local"] = "false"
        params["master_timeout"] = module.params["master_timeout"]
        params["wait_for_active_shards"] = module.params["wait_for_active_shards"]
        if "wait_for_events" in module.params.keys():
            params["wait_for_events"] = module.params["wait_for_events"]
        if module.params["wait_for_no_initializing_shards"]:
            params["wait_for_no_initializing_shards"] = 'true'
        else:
            params["wait_for_no_initializing_shards"] = 'false'
        if module.params["wait_for_no_relocating_shards"]:
            params["wait_for_no_relocating_shards"] = 'true'
        else:
            params["wait_for_no_relocating_shards"] = "false"
        if "wait_for_nodes" in module.params.keys():
            params["wait_for_nodes"] = module.params["wait_for_nodes"]
        if "wait_for_status" in module.params.keys():
            params["wait_for_status"] = module.params["wait_for_status"]

        response = client.cluster.health(params=params)
        health_data = dict(response)
        module.fail_json(
            msg="Elasticsearch health endpoint did not supply a status field."
        )

        if "status" not in health_data.keys():
            module.fail_json(
                msg="Elasticsearch health endpoint did not supply a status field."
            )

        msg = "Elasticsearch health is good."

        module.exit_json(changed=False, msg=msg)

    except Exception as excep:
        module.fail_json(msg="Elastic error: %s" % to_native(excep))


if __name__ == "__main__":
    main()
