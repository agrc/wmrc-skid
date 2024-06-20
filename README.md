# wmrc-skid

[![Push Events](https://github.com/agrc/wmrc-skid/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/wmrc-skid/actions/workflows/push.yml)

An automated updater for updating the hosted feature services behind the Department of Waste Management and Radiation Control (WMRC)'s recycling facilities [map](https://deq.utah.gov/waste-management-and-radiation-control/statewide-recycling-data-initiative) and dashboard.

## Overview

The map is an Experience Builder app that lives in DEQ's AGOL org. The dashboard is currently being built in their org as well. This skid updates five hosted feature services in their org:

- facilities
- county summaries
- statewide metrics
- materials recycled
- materials composted

It pulls data from a Google Sheet (facility ids, used oil collection center [UOCC] locations and amounts) and DEQ's Salesforce organization.

It is designed to run weekly as a Google Cloud Function.
