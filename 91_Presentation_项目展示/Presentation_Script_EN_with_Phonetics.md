# 3‑minute Presentation Script

> Project: **Operational Data Integration and Visualization Improvement**

---

Good afternoon everyone.
Today I will share our project: **Operational Data Integration and Visualization Improvement**.

## 1. Background and goal

In our daily work, we use many different tools – Microsoft Project, Planisware, Teams Planner and others – to manage projects and improvement activities.
Because these tools are not well connected, our data is often **inconsistent, not standard, and hard to trust**. This also makes management reporting slow and manual.

Our main goal in this project is:
- to **standardize operational data**,  
- to have **one trusted view of key KPIs**,  
- and to **improve reporting efficiency** for both engineers and managers.

---

## 2. Key problems from Measure phase

In the Measure phase, we looked at how data is collected and how reports are built.
We focused on four main issues.

**First**, **data is scattered and not standard.**  
Data sits in Power BI, Excel, SFC, OEE, MES, email and SharePoint.
Names and formats are different, and people spend a lot of time combining files by hand.

**Second**, **terms and KPIs are not clearly defined**.  
Different teams use different rules and formulas.  
The same KPI can show different numbers in different reports, so people are not sure which one is correct.

**Third**, **data quality and consistency are not well controlled.**  
There are missing and wrong records, and we do not have strong automatic checks across systems.  
Many important numbers still come from manual Excel and email files.

**Fourth**, **reporting is hard to scale and not well governed.**  
Power BI full refresh struggles with large data volume.  
Many similar reports are built by different teams, so it is hard to keep them in sync and to know which report to use.

The mind map on the slide shows how these issues are linked across data, process and reporting.

---

## 3. Main solutions from Analyze phase

Based on these findings, we designed four main solutions.

**Number one – standardize configuration and dictionaries.**  
We will build a unified configuration center and dictionary tables for fields, dimensions and keys,  
so all systems and reports can use the same product, process and equipment definitions.

**Number two – document data definitions and KPI rules.**  
We will create clear data platform documentation, and a DAX–ETL mapping table for key metrics,  
so everyone can see how each KPI is calculated.

**Number three – build data quality checks and consistency gates.**  
We will add error checks and alerts during data cleansing and calculation,  
and run a consistency replay before publishing reports, to make sure the numbers are trustworthy.

**Number four – make reporting scalable with offline processing and incremental refresh.**  
We will move heavy data processing off Power BI, use compression and incremental refresh,  
and let Power BI focus on display and interaction.

The architecture picture on the right shows how these four solutions connect source systems, the data platform, and Power BI.

---

## 4. Next steps

In the Improve and Control phases, we will implement these designs step by step,  
monitor the data accuracy rate,  
and make sure the new process is stable and sustainable.

That is my overview.  
Thank you, and I’m happy to take questions.
