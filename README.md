# Gimbal

Gimbal is a software storage switch for a SmartNIC-based disaggregated storage system implmeneted over SPDK.

## SIGCOMM 2021 Paper

https://dl.acm.org/doi/10.1145/3452296.3472940


### Enabling Multi-tenant Storage Disaggregation on SmartNIC JBOFs

Emerging SmartNIC-based disaggregated NVMe storage has become
a promising storage infrastructure due to its competitive
IO performance and low cost. These SmartNIC JBOFs are shared
among multiple co-resident applications, and there is a need for
the platform to ensure fairness, QoS, and high utilization. Unfortunately,
given the limited computing capability of the SmartNICs
and the non-deterministic nature of NVMe drives, it is challenging
to provide such support on today’s SmartNIC JBOFs. \
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

## Instruction

Gimbal is developed on SPDK v19.10.1 and Broadcom Stingray SmartNIC platform. 
However, it is completely independent of the platform and supports x86 and etc.
You can follow the instruction of SPDK to build and run Gimbal. \
https://github.com/spdk/spdk/tree/v19.10.1

## Notes

Gimbal has a design that has a dedicated pipeline for each SSD device, but does not restrict the user to run only one thread for an SSD. \
Instead, it is recommended to specify a core mask on the command line and only one SSD in the configuration file so that the Gimbal instance runs only for that one. \
To evaluate multiple SSDs, you may run multiple Gimbal instances for each SSD.

```bash
app/nvmf_tgt/nvmf_tgt -m 2 --json=<config file>
```

We will update Gimbal to automatically map thread and core to one SSD in the future.

## Contact

Jaehong Min <jaehongm@cs.washington.edu>
