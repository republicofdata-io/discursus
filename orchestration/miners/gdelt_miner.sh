#!/bin/bash

#Get url towards latest GDELT update
echo "Get meta info from GDELT"

content_regex="export.CSV.zip"
content=$(curl -v --silent http://data.gdeltproject.org/gdeltv2/lastupdate.txt --stderr - | grep $content_regex)

IFS=' ' read -a content_components <<< "$content"
latest_gdelt_url="${content_components[2]}"


#Get name of compressed file
IFS='/' read -a url_components <<< "$latest_gdelt_url"
compressed_file_name="${url_components[4]}"


#Get name of csv file
IFS='.' read -a file_components <<< "$compressed_file_name"
csv_file_name="${file_components[0]}.${file_components[1]}.${file_components[2]}"
file_name="${file_components[0]}.${file_components[1]}.txt"


#Download and extract latest events
echo "Downloading and extracting latest events"

curl $latest_gdelt_url > data/gdelt_tmp/$compressed_file_name
unzip -p "data/gdelt_tmp/$compressed_file_name" $csv_file_name > data/gdelt_tmp/$file_name


#Save gdelt data to S3
#echo "Copying to S3"

#file_location="data/gdelt_tmp/$file_name"
#aws s3 cp $file_location s3://discursus-io/sources/gdelt/$csv_file_name


#Delete local files
#echo "Cleaning up"

#rm data/gdelt_tmp/*
