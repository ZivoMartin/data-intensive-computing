1.​ What is the primary entity represented by the dataset?
2.​ Which attributes uniquely identify a record (i.e., what is the primary key of the
dataset)?
3.​ Which attributes are likely to be used for joins?
4.​ Which attributes are temporal?
5.​ Which attributes contain categorical values?
6.​ Which attributes are likely to grow over time?

# Taxi Trips

* Primary entity: A single yellow taxi trip.
* Primary key: None. The dataset does not provide an attribute or combination of attributes guaranteed to uniquely identify a trip.
* Join attributes: `PULocationID`, `DOLocationID`, and `tpep_pickup_datetime`. The location IDs can be joined with the Taxi Zone Lookup dataset, while the pickup timestamp can be used to associate trips with weather and air-quality observations.
* Temporal attributes: `tpep_pickup_datetime`, `tpep_dropoff_datetime`.
* Categorical values: `VendorID`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`.
* Likely to grow over time: The number of trip records grows continuously as new taxi trips are recorded.

# Weather

* Primary entity: An hourly weather observation.
* Primary key: (`year`, `month`, `day`, `hour`), assuming the dataset contains observations for a single location.
* Join attributes: `year`, `month`, `day`, and `hour`. These can be combined into a timestamp and matched with the hour of `tpep_pickup_datetime` in the Taxi Trips dataset.
* Temporal attributes: `year`, `month`, `day`, `hour`.
* Categorical values: The `*_source` attributes (`temp_source`, `rhum_source`, `prcp_source`, `snwd_source`, `wdir_source`, `wspd_source`, `wpgt_source`, `pres_source`, `cldc_source`, `coco_source`) represent categorical source/quality information. `coco` is also categorical.
* Likely to grow over time: The number of weather observations grows as new hourly measurements are recorded.

# Taxi Zone Lookup

* Primary entity: A taxi zone in New York City.
* Primary key: `LocationID`.
* Join attributes: `LocationID`, which can be joined with `PULocationID` and `DOLocationID` from the Taxi Trips dataset.
* Temporal attributes: None.
* Categorical values: `Borough`, `Zone`, `service_zone`.
* Likely to grow over time: The dataset is a lookup table and is not expected to grow significantly over time. It may only change when taxi zones or their classifications are modified.


# Air Quality

* Primary entity: An air-quality measurement for a specific pollutant, monitoring site, and time.
* Primary key: A composite key consisting of the monitoring site (`State Code`, `County Code`, `Site Num`), pollutant (`Parameter Code`), instrument/measurement occurrence (`POC`), and observation time (`Date Local`, `Time Local`). 
* Join attributes: `Date Local` and `Time Local` for temporal joins with taxi trips. `Latitude` and `Longitude` can be used for spatial association with taxi zones if needed.
* Temporal attributes: `Date Local`, `Time Local`, `Date GMT`, `Time GMT`, `Date of Last Change`.
* Categorical values: `State Code`, `County Code`, `Site Num`, `Parameter Code`, `POC`, `Datum`, `Parameter Name`, `Units of Measure`, `Qualifier`, `Method Type`, `Method Code`, `Method Name`, `State Name`, `County Name`.
* Likely to grow over time: The number of air-quality measurement records grows as monitoring stations continuously collect new observations.
