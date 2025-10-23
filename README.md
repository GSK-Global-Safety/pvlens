# pvlens
FDA SPL extraction pipeline

PVLens: Structured Product Label Extraction Framework
=====================================================


## Overview
PVLens is an open-source framework for extracting Indications, Adverse Events, Boxed Warnings, and related sections from FDA Structured Product Label (SPL) data.
 
It links extracted terms to standardized terminologies in the UMLS (MedDRA, SNOMED CT, RxNorm, ATC, etc.) and generates SQL output suitable for loading into a local database.

**Important:** 
PVLens distributes *only source code and helper methods*. 
It does **not** include any FDA, UMLS, or proprietary data. 
Users are responsible for obtaining and using these data sources in compliance with their respective licenses.


## License
This software is released under the **GNU General Public License, Version 3 (GPL v3)**.
A copy of the license is included with this distribution.

## Installation Notes
---------------------------------------------------------------------
1. Requirements
---------------------------------------------------------------------

* Java 17 or higher
* Apache Maven 3.8+ build tool
* A local installation of the UMLS with the minimum following vocabularies:
    - MedDRA
    - NDC
    - ATC
    - RxNorm
    - SNOMED CT (U.S. edition)
    - MTHSPL

* A local SQL database (MySQL or similar) for storing results
* A local copy of the SPL (Structured Product Label) XML archives

---------------------------------------------------------------------
2. Setup and Configuration
---------------------------------------------------------------------

All runtime configuration is managed through:

    src/main/resources/config/spldb.properties

Edit this file to match your local environment, including:
    - Database connection information for UMLS and PVLens databases
    - File system paths to your local SPL archive folders
    - Parallel processing limits and other runtime options

An additional configuration file is available:

    src/main/resources/config/loinc-filter.properties

This file defines which SPL sections are included from the "Other" category
using LOINC codes. Default settings are provided and are recommended unless
you are familiar with the SPL XML structure.

---------------------------------------------------------------------
3. Data Preparation
---------------------------------------------------------------------

1) **UMLS**
   You must have a local installation of the UMLS (minimum 2025AA or newer)
   with the vocabularies listed above loaded into MySQL or another RDBMS.

   PVLens expects the UMLS tables (MRCONSO, MRREL, MRSTY, etc.) to exist
   in a schema accessible via the JDBC connection defined in `spldb.properties`.

2) **SPL Data**
   Download the full SPL archives (DailyMed data) from the NIH website:
       https://dailymed.nlm.nih.gov/dailymed/spl-resources-all-drug-labels.cfm

   Unpack the archives on a **fast local disk (SSD/NVMe)**. File I/O is
   intensive — slower disks will greatly increase processing time.

   A recommended folder layout is:

       /data/spl_archive/
           ├── 2006/
           │   └── xml_files/
           ├── 2013/
           ├── 2020/
           └── 2025/

   For convenience, we have created a comprehensive archive of all SPL
   data from 2006-August 2025 that follows this file structure. These archives
   were cleaned and organized into otc, prescription and other by year. In addition,
   they have been redacted to only include the XML and removed all figures
   to conserve file space.  The archives require approx. 9gb of space, and when decompressed
   an additional 68gb of space is required. Upon processing, the complete database
   will require another 5gb of space to store the SQL files for loading into
   the MySQL instance.
   
   You can download those archives from:
   
   https://drive.google.com/drive/folders/1x818HtuLSk9UB_vxqo5GJFcEi4FyKxTy?usp=sharing
   
   Files:
   - spl_archives_2006-2012.tar.gz (800mb)
   - spl_archives_2013-2018.tar.gz (3.32gb)
   - spl_archives_2019-2022.tar.gz (3.52gb)
   - spl_archives_2023-2025.tar.gz (1.37gb)

3) **SRLC (Safety Related Label Change) database**

   There is a Python script located in python/srlc/download_srlc_data.py 
   when you clone the project. You should run this script from the 
   directory:
   
   $ cd python/srlc/
   $ python3 download_srlc_data.py
   
   This will take 5-10 minutes to run and will download the latest version
   of the SRLC website and store these as HTML files in the folder
   src/main/resources/fda_srlc/html_files/
   
   This script will download just over 3,000 files and requires 
   about 150mb of disk space.  The script uses a timer to avoid
   hitting the SRLC database too quickly and avoid getting dropped 
   connections. If you are experiencing trouble downloading, you
   may want to increase the wait time between calls.
   
   
---------------------------------------------------------------------
4. Build Instructions
---------------------------------------------------------------------

A build and run script have been included to simplify the build process.

   $ ./build.sh

or equivalently:

   $ mvn clean compile assembly:single

The compiled JAR will appear in the `target` directory.

Once the JAR file is built, you can then run the script to begin processing.

   $ ./run.sh
   
This will generate log output to the file in the directory:

   pvlens-spl-db-build.log

You can monitor the status by using the command (in a separate terminal)

   $ tail -f pvlens-spl-db-build.log

The build process currently takes about 45 minutes on a server with the 
following specifications:

   - CPU: AMD Ryzen 9 7950X 16-Core Processor
   - RAM: 32gb (minimum)
   - Disk: NVMe M.2 Samsung SSD 970 EVO Plus 2TB
   - OS: Ubuntu 24.04.3 LTS
   - Maven: Apache Maven 3.9.10
   - Java version: 17.0.9, vendor: Oracle Corporation

During execution PVLens will:

   - Read each SPL XML from the configured archive
   - Extract NDC codes, indications, and adverse reactions
   - Apply NLP-based context filtering and mapping
   - Generate portable SQL files for downstream database loading

All SQL output is written to:

   output/sql/

Each SPL section type (Indications, Adverse Reactions, Boxed Warnings, etc.)
is exported as a separate file. The generated SQL statements use plain
ANSI-quoted literals and can be batch-imported into MySQL or other databases.

---------------------------------------------------------------------
6. Database Loading
---------------------------------------------------------------------

After processing completes, you can load the SQL output into your local
database by following these steps:

   1) Load the PVLens database structure found in src/main/resources/sql
      Update the build.sh to enter your local MySQL username and password.
      Connect to your local MySQL instance and create the initial database:
      
      mysql> create database pvlens;

      Then run the build.sh script which will load the empty database
      structure for you.
      
   2) Next, change to the output/sql/ directory
      Run the ./build.sh script which will load the generated SQL
      into the database. If you encounter any errors during this
      step, please report those back to the PVLens team. We have
      tested thoroughly, but as data updates occur, things could
      break. The build script will load the data in the order
      required to meet foreign key relation requirements.

      
---------------------------------------------------------------------
7. Performance Tips
---------------------------------------------------------------------

* Use SSD or NVMe storage for SPL data to avoid I/O bottlenecks.
* Adjust the `parallel.maxThreads` setting in `spldb.properties`
  to match the number of CPU cores available.
* Ensure that your MySQL `max_allowed_packet` is set to at least 32M.
* For large runs, monitor available disk space in `output/sql/`.

---------------------------------------------------------------------
8. Troubleshooting
---------------------------------------------------------------------

* If the program appears slow or CPU usage drops to 1–2 cores,
  the SPL data may reside on a slow or remote drive.
* If MySQL reports syntax errors during import, verify that
  the generated `.sql` lines end with `);` and that your file
  encoding is UTF-8 (PVLens writes UTF-8 by default).
* Ensure your UMLS installation includes all required vocabularies;
  missing MedDRA or MTHSPL tables can lead to incomplete mappings.


## Contact and Support

For questions, issues, or contributions, please use the project’s GitHub Issues page

## Data Source and Licensing Notices

### FDA SPL and SRLC Data
- Public domain under U.S. Federal Government works (17 U.S.C. § 105). 
- Derived from publicly available SPL XML and SRLC HTML archives on [DailyMed](https://dailymed.nlm.nih.gov/). 
- Contains manufacturer-provided content (including trademarks and phone numbers). PVLens does **not** modify or republish these materials.

### UMLS / MedDRA / SNOMED CT / RxNorm / ATC
- **Not included** with PVLens. 
- Access and redistribution are subject to licensing terms from the National Library of Medicine (UMLS) and the International Federation of Pharmaceutical Manufacturers and Associations (MedDRA). 
- You must obtain your own UMLS license and ensure compliance before running PVLens.

### Stopword List Attribution
Portions of the English stopword list are derived from: 
**LHNCBC / fda-ars** — `MetaMapFiles/DATA/StopList` 
Source: [https://github.com/LHNCBC/fda-ars](https://github.com/LHNCBC/fda-ars) 
License: BSD-style with U.S. Government usage rights. 

“Courtesy of the U.S. National Library of Medicine.” 
Additional stop words were added by the PVLens team and are © 2025 GlaxoSmithKline, licensed under GPL v3.

### NLP Models
OpenNLP sentence and token models (`en-sent.bin`, `en-token.bin`) are distributed under the **Apache License 2.0**. 
Source: [https://opennlp.apache.org](https://opennlp.apache.org)

## Citations

If you use PVLens in your research, please cite:

Painter, J.L., Powell, G.E., & Bate, A. (2025).
PVLens: Enhancing pharmacovigilance through automated label extraction.
AMIA Annual Symposium Proceedings, 2025, Atlanta, GA.
DOI: 10.48550/arXiv.2503.20639

bibtex:

@article{pvlens2025,
  author       = {Painter, J.L. and Powell, G.E. and Bate, A.},
  title        = {{PVLens: Enhancing pharmacovigilance through automated label extraction}},
  journal      = {AMIA Annual Symposium Proceedings},
  publisher    = {AMIA},
  volume       = {2025},
  month        = {11},
  address      = {Atlanta, GA},
  type         = {Paper},
  doi          = {10.48550/arXiv.2503.20639}
}


---------------------------------------------------------------------

(c) 2025 GlaxoSmithKline. Licensed under the GPL v3.
