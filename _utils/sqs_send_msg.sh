curl -X POST \
  http://localhost:19324/queue/wmes-shef-service \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'Action=SendMessage&MessageBody=Hello%20from%20curl'

