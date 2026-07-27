**Briefing for the Solace File Connector Architect** · *product evolution request*

## 1. Context

Utech is deploying, on top of the **Solace File Connector (Micro-Integration)** and **PubSub+**, a replacement for an existing MediaTransfer-style file transfer solution. The requirement is a **bidirectional file flow** between a central site and a large fleet of Points of Sale (**POS**): files flowing **down** (central → POS) and **up** (POS → central).

Backends: **GCS** at the central site, **SFTP / FTP** at each POS.

The customer has **170 distinct flows**: **55% central → POS**, the remaining **45% POS → central**.

## 2. Target architecture

*(see attached diagram: `architecture-cible.svg`)*

**Central (GCP)** — one **PubSub+ Cloud broker (10k service class)**, a **pool of N File Micro-Integrations**, storage on **GCS**.

**POS (× 1500)** — per point of sale: **one edge broker** on a **Rocky Linux VM**, a **local Micro-Integration**, storage on **SFTP / FTP**. A **VPN Bridge** connects each edge broker to the central broker, **initiated by the edge/local broker**.

## 3. Test scenarios (boundary cases)

| # | Use case |Direction | Sources | Destinations | File size | Aggregate volume |
|---|----------|----------|:-------:|:-------------:|:---------:|:-----------------:|
| 1 | U02 |Central → POS — **fan-out** | 1 | 1500 | 30 MB | ~45 GB |
| 2 | U62 |Central → POS — **fan-out** | 1 | 500 | 200 MB | ~100 GB |
| 3 | U12 |POS → Central — **fan-in** | 1500 | 1 | 2 MB | ~3 GB |

## 4. Volume & constraints

- **Scale**: up to **1500 POS**; fan-out **1 → 1500** and fan-in **1500 → 1**.
- **File sizes**: from **2 MB** to **200 MB**.
- **Per-POS ordering**: **strong requirement** — for a given POS, files must be delivered / processed in emission order.
- **SLAs**: not yet defined at this stage (to be scoped later).

## 5. Expected coverage

Utech expects the File Connector to cover the following:

1. **Massive fan-out 1 → N** (N up to 1500) file distribution, byte-exact transfer.
2. **Massive fan-in N → 1** (N up to 1500) toward a single central endpoint.
3. **Per-POS ordering preservation** in both directions.
4. **Handling the target volumes**: large files (200 MB) as well as a large number of sources/destinations.
5. **Operation across the edge broker + VPN bridge topology** described in §2.
6. **Heterogeneous backends** GCS ↔ SFTP / FTP on both ends.
7. *(Optional)* **File compression** capability, to reduce the volume transferred over the link between edge and central.

## 6. Points to scope with the architect

- Sizing of the **Micro-Integration pool** at the central site (value of N) for each scenario.
- Recommended approach to guarantee **per-POS ordering** at scale.
- Behavior of the **10k broker** and the **1500 VPN bridges** under the volumes above.
- Expected behavior and best practices for **concurrent fan-in** toward a single central target, and for **fan-out** toward 500–1500 destinations.
