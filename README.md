# JSON_watcher
The "Observer" is watching for a new *.json files in source folder. When a new file appears, then writing to the Redis occurs. 
"Consumer" takes record from Redis, move file to another destination folder and make record to Postgresql. 