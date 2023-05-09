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
  timeout:
    description:
      - Period to wait for a response.
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
        timeout=dict(type="str", default="30s"),
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
        params["local"] = module.params["local"]
        params["master_timeout"] = module.params["master_timeout"]
        params["timeout"] = module.params["timeout"]
        params["wait_for_active_shards"] = module.params["wait_for_active_shards"]
        if "wait_for_events" in module.params.keys():
            params["wait_for_events"] = module.params["wait_for_events"]
        params["wait_for_no_initializing_shards"] = module.params[
            "wait_for_no_initializing_shards"
        ]
        params["wait_for_no_relocating_shards"] = module.params[
            "wait_for_no_relocating_shards"
        ]
        if "wait_for_nodes" in module.params.keys():
            params["wait_for_nodes"] = module.params["wait_for_nodes"]
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

# Cluster health APIedit
# Returns the health status of a cluster.

# Requestedit
# GET /_cluster/health/<target>

# Prerequisitesedit
# If the Elasticsearch security features are enabled, you must have the monitor or manage cluster privilege to use this API.
# Descriptionedit
# The cluster health API returns a simple status on the health of the cluster. You can also use the API to get the health status of only specified data streams and indices. For data streams, the API retrieves the health status of the stream’s backing indices.

# The cluster health status is: green, yellow or red. On the shard level, a red status indicates that the specific shard is not allocated in the cluster, yellow means that the primary shard is allocated but replicas are not, and green means that all shards are allocated. The index level status is controlled by the worst shard status. The cluster status is controlled by the worst index status.

# One of the main benefits of the API is the ability to wait until the cluster reaches a certain high water-mark health level. For example, the following will wait for 50 seconds for the cluster to reach the yellow level (if it reaches the green or yellow status before 50 seconds elapse, it will return at that point):

# GET /_cluster/health?wait_for_status=yellow&timeout=50s

# Path parametersedit
# <target>
# (Optional, string) Comma-separated list of data streams, indices, and index aliases used to limit the request. Wildcard expressions (*) are supported.

# To target all data streams and indices in a cluster, omit this parameter or use _all or *.

# Query parametersedit
# level
# (Optional, string) Can be one of cluster, indices or shards. Controls the details level of the health information returned. Defaults to cluster.
# local
# (Optional, Boolean) If true, the request retrieves information from the local node only. Defaults to false, which means information is retrieved from the master node.
# master_timeout
# (Optional, time units) Period to wait for a connection to the master node. If no response is received before the timeout expires, the request fails and returns an error. Defaults to 30s.
# timeout
# (Optional, time units) Period to wait for a response. If no response is received before the timeout expires, the request fails and returns an error. Defaults to 30s.
# wait_for_active_shards
# (Optional, string) A number controlling to how many active shards to wait for, all to wait for all shards in the cluster to be active, or 0 to not wait. Defaults to 0.
# wait_for_events
# (Optional, string) Can be one of immediate, urgent, high, normal, low, languid. Wait until all currently queued events with the given priority are processed.
# wait_for_no_initializing_shards
# (Optional, Boolean) A boolean value which controls whether to wait (until the timeout provided) for the cluster to have no shard initializations. Defaults to false, which means it will not wait for initializing shards.
# wait_for_no_relocating_shards
# (Optional, Boolean) A boolean value which controls whether to wait (until the timeout provided) for the cluster to have no shard relocations. Defaults to false, which means it will not wait for relocating shards.
# wait_for_nodes
# (Optional, string) The request waits until the specified number N of nodes is available. It also accepts >=N, <=N, >N and <N. Alternatively, it is possible to use ge(N), le(N), gt(N) and lt(N) notation.
# wait_for_status
# (Optional, string) One of green, yellow or red. Will wait (until the timeout provided) until the status of the cluster changes to the one provided or better, i.e. green > yellow > red. By default, will not wait for any status.
# Response bodyedit
# cluster_name
# (string) The name of the cluster.
# status
# (string) Health status of the cluster, based on the state of its primary and replica shards. Statuses are:

# green: All shards are assigned.
# yellow: All primary shards are assigned, but one or more replica shards are unassigned. If a node in the cluster fails, some data could be unavailable until that node is repaired.
# red: One or more primary shards are unassigned, so some data is unavailable. This can occur briefly during cluster startup as primary shards are assigned.
# timed_out
# (Boolean) If false the response returned within the period of time that is specified by the timeout parameter (30s by default).
# number_of_nodes
# (integer) The number of nodes within the cluster.
# number_of_data_nodes
# (integer) The number of nodes that are dedicated data nodes.
# active_primary_shards
# (integer) The number of active primary shards.
# active_shards
# (integer) The total number of active primary and replica shards.
# relocating_shards
# (integer) The number of shards that are under relocation.
# initializing_shards
# (integer) The number of shards that are under initialization.
# unassigned_shards
# (integer) The number of shards that are not allocated.
# delayed_unassigned_shards
# (integer) The number of shards whose allocation has been delayed by the timeout settings.
# number_of_pending_tasks
# (integer) The number of cluster-level changes that have not yet been executed.
# number_of_in_flight_fetch
# (integer) The number of unfinished fetches.
# task_max_waiting_in_queue_millis
# (integer) The time expressed in milliseconds since the earliest initiated task is waiting for being performed.
# active_shards_percent_as_number
# (float) The ratio of active shards in the cluster expressed as a percentage.
# Examplesedit
# GET _cluster/health

# Console
# Copy as curl
# View in Console

# The API returns the following response in case of a quiet single node cluster with a single index with one shard and one replica:

# {
#   "cluster_name" : "testcluster",
#   "status" : "yellow",
#   "timed_out" : false,
#   "number_of_nodes" : 1,
#   "number_of_data_nodes" : 1,
#   "active_primary_shards" : 1,
#   "active_shards" : 1,
#   "relocating_shards" : 0,
#   "initializing_shards" : 0,
#   "unassigned_shards" : 1,
#   "delayed_unassigned_shards": 0,
#   "number_of_pending_tasks" : 0,
#   "number_of_in_flight_fetch": 0,
#   "task_max_waiting_in_queue_millis": 0,
#   "active_shards_percent_as_number": 50.0
# }
# The following is an example of getting the cluster health at the shards level:

# GET /_cluster/health/my-index-000001?level=shards
