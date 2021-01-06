#! /bin/sh

TOTAL_SEGMENTS=$1
SEGMENT_NUMBER=$2
echo $SEGMENT_NUMBER $TOTAL_SEGMENTS

SCAN_OUTPUT="scan-output-segment${SEGMENT_NUMBER}.json"
SCAN_AGGREGATE="scan-agg-segment${SEGMENT_NUMBER}.json"

aws dynamodb scan --table-name "TestDB" --projection-expression "userId" --filter-expression "timeStamp >= :start_date and timeStamp <= :end_date" --expression-attribute-values file://attribute-values.json --max-items "1000" --total-segments "${TOTAL_SEGMENTS}" --segment "${SEGMENT_NUMBER}" > ${SCAN_OUTPUT} --region ap-southeast-1

NEXT_TOKEN="$(cat ${SCAN_OUTPUT} | jq '.NextToken')"

cat ${SCAN_OUTPUT} | jq -r ".Items[] | tojson" > ${SCAN_AGGREGATE}

while [ ! -z "$NEXT_TOKEN" ] && [ ! "$NEXT_TOKEN" == null ]
do
aws dynamodb scan --table-name "TestDB" --projection-expression "userId" --filter-expression "timeStamp >= :start_date and timeStamp <= :end_date" --expression-attribute-values file://attribute-values.json --max-items "1000" --total-segments "${TOTAL_SEGMENTS}" --segment "${SEGMENT_NUMBER}" --starting-token "${NEXT_TOKEN}" > ${SCAN_OUTPUT} --region ap-southeast-1

NEXT_TOKEN="$(cat ${SCAN_OUTPUT} | jq '.NextToken')"

cat ${SCAN_OUTPUT} | jq -r ".Items[] | tojson" >> ${SCAN_AGGREGATE}

done

SEGMENT_FILE="delete-segment${SEGMENT_NUMBER}.json"
MAX_ITEMS=25

printf "starting segment - ${SEGMENT_NUMBER} \n" > ${SEGMENT_FILE}
until [[ ! -s ${SEGMENT_FILE} ]] ;
do
awk "NR>${CNT:=0} && NR<=$((CNT+MAX_ITEMS))" ${SCAN_AGGREGATE} | awk '{ print "{\"DeleteRequest\": {\"Key\": " $0 "}}," }' | sed '$ s/.$//' | sed '1 i { "TestDB": [' | sed '$ a ] }'> ${SEGMENT_FILE}

aws dynamodb batch-write-item --request-items file://${SEGMENT_FILE} --region ap-southeast-1
CNT=$((CNT+MAX_ITEMS))

done
