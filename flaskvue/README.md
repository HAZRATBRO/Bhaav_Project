########## Bhaav Equity Scheduler and API #########


SALIENT FEATURES :-

  1. A standalone application with a scheduler to fetch data from NSE BhaavCopy everyday at 6 p.m. and store data to Redis
  2. REST APIs to read data from REDIS store and return the loaded data.
  3. A Vue.js based Front End that can search for a Stock name and return the result as a table.

How To Install :-

  1. If on windows run install.bat else run install.sh for linux
  2. Goto backend and start app.py
  3. The Application should be running at localhost:5000
  4. Ensure you have a redis instance running on your local
   