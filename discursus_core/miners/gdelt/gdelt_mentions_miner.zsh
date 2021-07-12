#!/bin/zsh
export PATH=/usr/local/bin:$PATH

#Get url towards latest GDELT update
echo "Get meta info from GDELT"

content_regex="mentions.CSV.zip"
content=$(curl -v --silent http://data.gdeltproject.org/gdeltv2/lastupdate.txt --stderr - | grep $content_regex)

IFS=' ' read -A content_components <<< "$content"
latest_gdelt_url="${content_components[3]}"


#Get name of compressed file
IFS='/' read -A url_components <<< "$latest_gdelt_url"
compressed_file_name="${url_components[5]}"
file_date=${compressed_file_name:0:8}


#Get name of csv file
IFS='.' read -A file_components <<< "$compressed_file_name"
csv_file_name="${file_components[1]}.${file_components[2]}.${file_components[3]}"
file_name="${file_components[1]}.${file_components[2]}.txt"


#Download and extract latest events
echo "Downloading and extracting latest events"

curl $latest_gdelt_url > $DISCURSUS_MINER_GDELT_HOME/tmp/$compressed_file_name
unzip -p "$DISCURSUS_MINER_GDELT_HOME/tmp/$compressed_file_name" $csv_file_name > $DISCURSUS_MINER_GDELT_HOME/tmp/$file_name


#Save gdelt data to S3
echo "Copying to S3"

file_location="$DISCURSUS_MINER_GDELT_HOME/tmp/$file_name"
aws s3 cp $file_location s3://discursus-io/sources/gdelt/$file_date/$csv_file_name


#Delete local files
echo "Cleaning up"


#Return path to latest saved mention file
echo "s3://discursus-io/sources/gdelt/$file_date/$csv_file_name"