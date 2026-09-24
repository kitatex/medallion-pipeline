# medallion-pipeline

I created a data pipeline to teach myself data engineering methods. The result is a simple webpage which ranks 7 outdoor destination in or near Vienna by how good the weather is on a certin day.

You can access the site [HERE](https://kitatex.github.io/medallion-pipeline/).

The site is a data engineering PoC, and hence the analytical part is not interesting at all.

## Technical Summary

**1:** A python script extracts weather forecast data from the Open-Meteo API and loads the raw .json directly into Azure Data Lake Storage (ADLS).

**2:** A Databricks PySpark job implements a Medallion architecture, processing the data through bronze, silver and gold layers (delta tables), resulting in a .json available for serving a frontend.

**3:** A Databricks Workflow schedules and runs the pipeline daily on a single-node cluster. A GitHub Action subsequently pulls the .json from ADLS into the repository, automatically updating a static HTML/JS frontend hosted on GitHub Pages.
