# healthcare-data-rest
A project to consume CMS data, and provide it as REST endpoints

# CMS Data Scraping

We need to scrape the following data from CMS...

But CMS download links are not stable, so we need to scrape the data from the website, for the link.

We will use https://crates.io/crates/chromiumoxide to scrape the download link from the website, and the last modified date of the file, so we don't have to download the file if it hasn't changed.

# Hospital Enrollments (+Crtical Access Hospitals, +Rural Emeregency Hospitals)
https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments

