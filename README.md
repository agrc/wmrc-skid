# wmrc-skid

[![Push Events](https://github.com/agrc/wmrc-skid/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/wmrc-skid/actions/workflows/push.yml)

An automated updater for updating the hosted feature services behind the Department of Waste Management and Radiation Control (WMRC)'s recycling facilities [map](https://deq.utah.gov/waste-management-and-radiation-control/statewide-recycling-data-initiative) and dashboard and also running validation analyses three times a year.

## Facilities Update

The map is an Experience Builder app that lives in DEQ's AGOL org. The dashboard is currently being built in their org as well. This skid updates five hosted feature services in their org:

- facilities
- county summaries
- statewide metrics
- materials recycled
- materials composted

It pulls data from a Google Sheet (facility ids, used oil collection center [UOCC] locations and amounts) and DEQ's Salesforce organization.

It is designed to run weekly as a Google Cloud Function.

### Renaming Recycled/Composted Categories

Some material names in the recycled/composted reports are abbreviated in the salesforce data. These (along with any other material names) can be renamed as part of the great renaming chain in the end of `yearly.rates_per_material()`.

## Validation Script

The validation script compares year-over-year changes for different metrics at the facility, county, and state levels to help WMRC staff identify potential typos, missing information, or other problems with the data.

It runs on the following schedule:

- April 1 of each year: First check
- May 1 of each year: Check for go-live
- May 20 of each year: Data from previous year live on map (validation script doesn't run, but reminder for us to change the year value in config.py and any needed filters on the map/dashboard)
- June 1 of each year: Final check

## Multiple Schedules, One Function

This skid is deployed as a single gen2 Cloud Function with two different schedules. Each Cloud Schedule should contain a different message-body: `'facility updates'` to trigger the feature service updating, and `'validate'` to trigger the validator script.

## Attribution

This project was developed with the assistance of [GitHub Copilot](https://github.com/features/copilot).
