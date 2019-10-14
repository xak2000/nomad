---
layout: "guides"
page_title: "Advanced Scheduling Features"
sidebar_current: "guides-advanced-scheduling"
description: |-
    Introduce advanced scheduling features including affinity and spread.
---

# Advanced Scheduling with Nomad

The Nomad [scheduler][scheduling] uses a bin packing algorithm to optimize the resource utilization and density of applications in your Nomad cluster. Nomad 0.9 adds new features to allow operators more fine-grained control over allocation placement. This enables use cases similar to the following:

- Expressing preference for a certain class of nodes for a specific application via the [affinity block][affinity-block].

- Spreading allocations across a datacenter, rack or any other node attribute or metadata with the [spread block][spread-block].

Please refer to the guides below for using affinity and spread in Nomad 0.9.

- [Affinity][affinity-guide]
- [Spread][spread-guide]

[affinity-guide]: /guides/operating-a-job/advanced-scheduling/affinity.html
[affinity-block]: /docs/job-specification/affinity.html
[spread-guide]: /guides/operating-a-job/advanced-scheduling/spread.html
[spread-block]: /docs/job-specification/spread.html
[scheduling]: /docs/internals/scheduling/scheduling.html

