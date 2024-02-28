# FROM flyway/flyway
FROM ghcr.io/usace/flyway:latest

COPY . /flyway/sql/

ENTRYPOINT [ "flyway" ]

CMD [ "-connectRetries=60", "migrate" ]