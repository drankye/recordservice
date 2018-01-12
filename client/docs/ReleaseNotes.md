---
layout: page
title: 'RecordService Beta Release Notes'
---

RecordService provides an abstraction layer between compute frameworks and data storage. It provides row- and column-level security, and other advantages as well.

## Platform/Hardware Support
* Server support: RHEL5-7, Ubuntu LTS, SLES, Debian
* Intel Nehalem (or later) or AMD  Bulldozer (or later) processor
* 64GB memory
* For optimal performance, run with 12 or more disks, or use SSD.

## Known Issues

### digest-md5 library not installed on cluster, breaking delegation tokens
To install the library on RHEL6 use the following command line instruction:

```
sudo yum install cyrus-sasl-md5
```

## Limitations
* Only supports simple single-table views (no joins or aggregations).
* SSL support has not been tested.
* Oozie integration has not been tested.
* No support for write path.
* Unable to read from Kudu or HBase.
* No diagnostic bundle support.
* No metrics available in CM.
* Spark DataFrame not well tested.


[overview]: {{site.baseurl}}/

