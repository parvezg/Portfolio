# Thoughtspot Self cleaning project

After we implented ThoughtSpot for our external customers in 2020, January and for internal usage in 2021, August the number of tables, answers and liveboards grew constantly. 
And of course it is human behaviour that any objects not used at the time will not be deleted.
by 1Q2025 we have:

-   289 tables, 91 used in last month
-   537 worksheets, 368 used in last month
-   11,877 answers, 6870 used in last month
-   1770 liveboards, 727 used in last month

Thus we implemented a process to clean ThoughtSpot automatically. That is ThoughtSpot becomes self-cleaning and ThoughtSpot objects (answers/liveboards) which are not accessed for 90 days or more will be deleted if not users do not contradict.

# Process Description
we identify ThoughtSpot objects (answers/liveboards) which are not accessed for 90 days or more,  their authors and the last 3 users
we show this list in ThoughtSpot and inform the authors and last 3 users that we are going to delete these objects 
if authors or users do not want to get objects deleted they simply need to access these objects.

#Technical Implementation

The process of ThoughtSpot self-cleaning mechanism follows these steps:

- The Airflow DAG is triggered, and it's first task sends and API call to retrieve the list of objects in ThoughtSpot that were not accessed for 90 days or more.
- This list is then compared against the table in Snowflake (DWH.REPORTING.THOUGHTSPOT_OBJECTS_DELETION_LIST_TBL). The objects that are present in both in the list and the table will not be added to the table again and are excluded from the list, and the objects that are present only in the table, but not in the list will be deleted form the table, since they were deleted from ThoughtSpot by previous iteration of the process.
- The list is then enriched with the information about 3 last users of each objects by another API call to ThoughtSpot.
- The final list is then uploaded to Snowflake into table DWH.REPORTING.THOUGHTSPOT_OBJECTS_DELETION_LIST_TBL. 
- The ThoughtSpot report is provided to users based on that table, also sending authors the notification about objects that are to be deleted.
- In ThoughtSpot the table is created based on this query: 

SELECT GUID, "TYPE", "NAME", AUTHOR, LAST_ACCESSED, "USER", NOTIFICATION_DATE, DELETION_DATE FROM DWH.REPORTING.THOUGHTSPOT_OBJECTS_DELETION_LIST_TBL

- In ThoughtSpot report on this table with Author_relevant_records_flag=1

- Author_relevant_records_flag = if(ts_username =author ) then 1 else 0 

<img width="1519" height="724" alt="Screenshot 2026-05-19 at 17 51 54" src="https://github.com/user-attachments/assets/fae970bb-d201-4c82-921e-86420be15c06" />


# How To Retrieve The Deleted Object Back To TS
 
 The process of retrieving deleted object back into TS as follows:

- Ask for the object name / guid and look for the same in (DWH.THOUGHTSPOT_METADATA.TML_TBL ).
- Retrieve the object TML and download it.
- Go to TS DATA > UTILITIES > Import/Export TML and upload the downloaded TML in step 2.
- validate the errors and submit it and click on exit, you will find the Object back in TS.
- Once object back into TS, transfer the ownership of the object to the rightful owner, so the owner of the objects gets notified of its usage.
