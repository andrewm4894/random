#!/bin/bash

# get list of file types to loop through
filelist=("-20-earnings-report-" "-20-orders-report-" "-20-tracking-report-")

# get list of lobs
loblist=("mecom00_me")

# set up date range
begindate=$(date --date="-7 days" +%Y%m%d)
todaydate=$(date --date="-0 days" +%Y%m%d)

# set some params
currentdate=$begindate
loopenddate=$(date --date "$todaydate 1 day" +%Y%m%d)

# loop through params and do work
until [ "$currentdate" == "$loopenddate" ]
do

  # loop through each file name
  for file in "${filelist[@]}"
  do
    
    # loop through each lob
    for lobstr in "${loblist[@]}"
    do
    
      # set up some variables
		  lob=`echo "$lobstr" | awk -F_ '{print $1}'`
		  lob_clean=`echo "$lobstr" | awk -F_ '{print $2}'`
    
      # print out where we are
      echo "--------------------------------------------"
      echo "BEGIN: "$lob$file$currentdate".tsv.gz"
      echo "--------------------------------------------"
            
      lobpart=$lob
      datepart=$currentdate
      filepart=$file
      filename=${lobpart}${filepart}${datepart}".tsv.gz" # the feed file we want to download
      filenameunzipped=${filename::-3}
      bqtargettablename=${lob_clean}${filepart}${datepart}
      # clean target table name to be acceptable table name in bq
      bqtargettablename=`echo ${bqtargettablename} | sed 's/-/_/g'`
      retries=5 # the number of times you want to retry
      success=1 # the status of the download
      outfilename=/home/tmp/$filename
      myfile=${outfilename::-3}
      rm $outfilename # remove the file if it already exists
      rm $myfile # remove the file if it already exists
      rm $myfile.clean # remove the file if it already exists

      echo 'attempting - '$outfilename

      while [ $retries -ne 0 -a $success -ne 0 ]
      do
      curl --location --user "usr":"pwd" -C - --digest -k https://assoc-datafeeds-na.amazon.com/datafeed/getReport?filename=$filename -o $outfilename
      success=$?
      retries=$(expr $retries - 1)
      done

      gunzip $outfilename
      # remove first row from amazon file
      tail -n +2 $myfile > $myfile.clean
      # overwrite with cleaned file
      cp $myfile.clean $myfile

      # make pipe delimited
      sed -i 's/\t/|/g' $myfile

      echo $myfile
      head $myfile
      
      # if not enough rows in file then move to next
      linecnt=$( cat $myfile | wc -l )
      echo '...line count is '$linecnt'...'
      if [ ${linecnt} -le 2 ]
      then
      
        echo '...SKIPPING TO NEXT AS EMPTY FILE...'
        # kill table if already exists in bq (as is probably rubbish)
        bq rm -f amazon.$bqtargettablename
        continue
        
      else
      
        # load to google cloud storage
        gsutil cp $myfile gs://my-amazon-associates-data
        # kill table if already exists in bq
        bq rm -f amazon.$bqtargettablename
        # load to bigquery
        bq load --autodetect --skip_leading_rows=1 -F '|'  amazon.$bqtargettablename gs://my-amazon-associates-data/$filenameunzipped
        # look at table meta to make sure ok
        bq head -n 5 amazon.$bqtargettablename

        # clean up
        rm $outfilename
        rm $myfile
        rm $myfile.clean
      
      fi 
      
    done
  
  done

  # increment date by 1 day
  currentdate=$(date --date "$currentdate 1 day" +%Y%m%d)

done
