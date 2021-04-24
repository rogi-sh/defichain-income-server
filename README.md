# defichain-income-server
0. add .env file example: 

DB_CONN = "db"
POOL_API = "https://api.defichain.io/v1/getpoolpair?id="
POOL_FARMING_API = "https://api.defichain.io/v1/listyieldfarming?network=mainnet"
STATS_API = "https://api.defichain.io/v1/stats"
JOB_SCHEDULER_ON = "off"
JOB_SCHEDULER_TURNUS = "*/10 * * * * *"
JOB_SCHEDULER_ON_STATS = "off"
JOB_SCHEDULER_TURNUS_STATS = "0 0,12/12 * * *"
DEBUG = "on"
AUTH = "key"

1. npm i
2. npm start


