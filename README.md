# Gimbal: Enabling Multi-tenant Storage Disaggregation on SmartNIC JBOFs

Emerging SmartNIC-based disaggregated NVMe storage has become
a promising storage infrastructure due to its competitive
IO performance and low cost. These SmartNIC JBOFs are shared
among multiple co-resident applications, and there is a need for
the platform to ensure fairness, QoS, and high utilization. Unfortunately,
given the limited computing capability of the SmartNICs
and the non-deterministic nature of NVMe drives, it is challenging
to provide such support on today’s SmartNIC JBOFs.

This paper presents Gimbal, a software storage switch that orchestrates
IO traffic between Ethernet ports and NVMe drives for
co-located tenants. It enables efficient multi-tenancy on SmartNIC
JBOFs using the following techniques: a delay-based SSD congestion
control algorithm, dynamic estimation of SSD write costs, a
fair scheduler that operates at the granularity of a virtual slot, and
an end-to-end credit-based flow control channel. Our prototyped
system not only achieves up to x6.6 better utilization and 62.6% less
tail latency but also improves the fairness for complex workloads.
It also improves a commercial key-value store performance in a
multi-tenant environment with x1.7 better throughput and 35.0%
less tail latency on average.
