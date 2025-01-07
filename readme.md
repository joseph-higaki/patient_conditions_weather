
# Patients Condition / Weather 

## Problem Statement
How clinical diagnoses correlate to weather patterns.

## Methodology
Iteratively
* Data Wrangle
* Hypothesize
* Extract insights or correlation 

Given a dataset of clinical conditions found in a list of clinical encounters (inpatient or outpatient) 

## Steps
### Extract / Ingest EMR data
* This is ingested from synthetic realistic data from [Synthetic Mass | Synthea](https://synthea.mitre.org/) 
* Conditions are represented [SNOMED CT](https://www.snomed.org/what-is-snomed-ct) codes
* FHIR formats are available for future improvements.
### Extract weather data
* From Geo located patient encounters and diagnosis date.
* Extract weather data from [Open-Meteo](https://open-meteo.com/) historical APIs

### Do Exploratory Data Analysis 
* [Top 3 conditions](notebooks/eda_top_conditions.ipynb)

## Data Pipeline 
![alt text](_resources/readme.md/synthea_seasonal_conditions_diagram.drawio.png)





[Using Fake but Realistic Data from Synthea](https://synthea.mitre.org/downloads)
> Jason Walonoski, Mark Kramer, Joseph Nichols, Andre Quina, Chris Moesel, Dylan Hall, Carlton Duffett, Kudakwashe Dube, Thomas Gallagher, Scott McLachlan, Synthea: An approach, method, and software mechanism for generating synthetic patients and the synthetic electronic health care record, Journal of the American Medical Informatics Association, Volume 25, Issue 3, March 2018, Pages 230–238, https://doi.org/10.1093/jamia/ocx079