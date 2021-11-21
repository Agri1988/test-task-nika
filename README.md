1. Run docker container with mysql 
```shell script
docker run --name mysql -d -p3306:3306 --env-file ./.env mysql:latest
```
#####If you early didn't build image you should do that with next command
```shell script
docker build . -t parser:latest
``` 
2. Run docker container which creates medical_codes table
```shell script
docker run --name create-table --rm --env-file ./.env parser:latest python db.py 
```
3. Run docker container which parses medical codes and inserts it into DB
```shell script
docker run --name parse-codes --rm --env-file ./.env parser:latest python parser.py 
```
